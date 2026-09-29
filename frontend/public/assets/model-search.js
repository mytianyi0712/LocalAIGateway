/**
 * 模型名模糊匹配：把查询串按 token 与候选模型名做加权打分并排序。
 *
 * 职责：为模型选择器提供相似度打分与排序，供 app.js 导入。
 * 边界：纯函数，不访问 DOM 与网络，可直接在 Node 下验证。
 * 关键不变量：低于「至少一个强命中且查询字符覆盖 > 50%」的候选直接丢弃。
 */
//
// 查询串按非字母数字切分为 token，再逐 token 对候选的 `model_id` 与
// `display_name` 打分：完全相同的 token 得 1，前缀 0.88，包含 0.62，
// 子序列 0.45。命中率按「查询 token 的字符数」加权，因此
// 查询 `deepseek-v4.1-flash` 时：
//   deepseek-v4.1-flash  → 100% 命中（精确匹配，排最前）
//   deepseek-v4.1-flash-0731 → 100% 命中（前缀接近）
//   deepseek-flash       → 81%  命中
//   deepseek-v4-pro      → 63%  命中
//   deepseek-chat        → 50%  命中（不过阈值，丢弃）

const SEPARATOR = /[^a-z0-9]+/;

// 命中率必须**超过**该比例：`deepseek-v4.1-flash` 查询下 `deepseek-flash`
// （0.81）、`deepseek-v4-pro`（0.63）留下，而只覆盖一半查询的
// `deepseek-chat`（0.50）等无关程度更高的模型被丢弃。
const MIN_SHARE = 0.5;
// 单个 token 至少达到该分数才算「强命中」，避免整串子序列凑出假匹配。
const MIN_SOLID_SCORE = 0.6;

export function searchTokens(value) {
  return String(value ?? '').toLowerCase().split(SEPARATOR).filter(Boolean);
}

function compactName(value) {
  return String(value ?? '').toLowerCase().replace(SEPARATOR, '');
}

function isSubsequence(needle, haystack) {
  if (!needle) return false;
  let index = 0;
  for (let position = 0; position < haystack.length && index < needle.length; position += 1) {
    if (haystack[position] === needle[index]) index += 1;
  }
  return index === needle.length;
}

function tokenScore(token, targetTokens, targetCompact) {
  let best = 0;
  for (const part of targetTokens) {
    if (part === token) return 1;
    if (part.startsWith(token)) best = Math.max(best, 0.88);
    else if (token.length >= 3 && token.startsWith(part)) best = Math.max(best, 0.7);
    else if (token.length >= 3 && part.includes(token)) best = Math.max(best, 0.62);
    else if (token.length >= 3 && isSubsequence(token, part)) best = Math.max(best, 0.45);
  }
  // 跨 token 的紧凑串兜底（如 `deepseekv4` 命中 `deepseek-v4`）。短片段
  // （`v4`、`1`）只认完全相同的 token，否则 `1` 会命中 `0731` 这类版本号。
  if (best < 0.5 && token.length >= 3 && targetCompact.includes(token)) best = 0.7;
  return best;
}

function scoreName(queryTokens, queryCompact, name) {
  const lower = String(name ?? '').toLowerCase();
  if (!lower) return null;
  const targetTokens = lower.split(SEPARATOR).filter(Boolean);
  const targetCompact = lower.replace(SEPARATOR, '');
  let total = 0;
  let matched = 0;
  let solid = 0;
  let shareWeight = 0;
  let qualityWeight = 0;
  for (const token of queryTokens) {
    const weight = token.length;
    total += weight;
    const score = tokenScore(token, targetTokens, targetCompact);
    if (score >= MIN_SOLID_SCORE) solid += 1;
    if (score >= 0.5) {
      matched += 1;
      shareWeight += weight;
    }
    qualityWeight += score * weight;
  }
  return {
    share: total ? shareWeight / total : 0,
    quality: total ? qualityWeight / total : 0,
    matched,
    solid,
    exact: targetCompact === queryCompact,
    prefix: targetCompact.startsWith(queryCompact),
  };
}

// 单个候选与查询串的相似度；无关（低命中率）返回 null。
export function modelMatchScore(query, item) {
  const queryTokens = searchTokens(query);
  if (!queryTokens.length) return null;
  const queryCompact = compactName(query);
  let best = null;
  for (const name of [item?.model_id, item?.display_name]) {
    const score = scoreName(queryTokens, queryCompact, name);
    if (!score) continue;
    if (!best || score.quality > best.quality || (score.quality === best.quality && score.share > best.share)) best = score;
  }
  if (!best || best.solid < 1 || !(best.share > MIN_SHARE)) return null;
  return best;
}

function compareMatches(a, b) {
  if (a.exact !== b.exact) return a.exact ? -1 : 1;
  if (a.prefix !== b.prefix) return a.prefix ? -1 : 1;
  if (b.quality !== a.quality) return b.quality - a.quality;
  if (b.share !== a.share) return b.share - a.share;
  if (b.matched !== a.matched) return b.matched - a.matched;
  const aLength = String(a.item?.model_id || '').length;
  const bLength = String(b.item?.model_id || '').length;
  if (aLength !== bLength) return aLength - bLength;
  return String(a.item?.model_id || '').localeCompare(String(b.item?.model_id || ''));
}

// 按相似度排序的候选列表；`limit > 0` 时截断（调用方负责提示剩余数量）。
export function rankModelMatches(query, items = [], { limit = 0 } = {}) {
  if (!searchTokens(query).length) {
    return (limit > 0 ? items.slice(0, limit) : [...items]).map((item) => ({ item, exact: false, prefix: false, quality: 0, share: 0, matched: 0 }));
  }
  const matches = [];
  for (const item of items) {
    const score = modelMatchScore(query, item);
    if (score) matches.push({ item, ...score });
  }
  matches.sort(compareMatches);
  return limit > 0 ? matches.slice(0, limit) : matches;
}
