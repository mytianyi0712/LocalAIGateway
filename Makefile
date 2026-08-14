# Local AI Gateway 构建入口。
#
# 双宿主构建模型：
#   - Windows 本机（rustup x86_64-pc-windows-gnu + mingw-w64）：构建 NSIS exe
#   - Arch WSL（WSL2 内的 Arch Linux，非 root builder 用户）：构建 ELF 产物
#     （AppImage、deb、.pkg.tar.zst）——Windows 宿主无法产出 Linux 二进制。
#
# 首次配置：make wsl-setup   （root 下安装 WSL 工具链 + builder 用户 + tauri-cli）
# 全部产物：make package-all
.PHONY: desktop-icons desktop-build-linux desktop-build-windows \
	package-appimage package-arch package-windows package-windows-wine package-all \
	version version-set version-check check-layers wsl-setup

WSL ?= wsl
WSL_BUILDER ?= builder
# 本仓库在 WSL 客机视角的路径（Linux 侧）。Windows 宿主执行 make 时经 wslpath 换算。
WSL_ROOT := $(shell $(WSL) -e wslpath '$(CURDIR)' 2>/dev/null)

# 在 Arch WSL 内以非 root builder 身份运行仓库内命令。makepkg 拒绝 root，
# Linux 构建统一走 builder 用户。
define wsl_build
	$(WSL) -e bash -lc 'su $(WSL_BUILDER) -c "cd $(WSL_ROOT) && $1"'
endef

desktop-icons:
	cd desktop/src-tauri && cargo tauri icon icons/icon.svg

## Linux 产物（AppImage + deb）在 Arch WSL 内构建。
desktop-build-linux:
	$(call wsl_build,./desktop/scripts/build-appimage.sh)

package-appimage:
	$(call wsl_build,./packaging/build.sh appimage)

## Arch .pkg.tar.zst 包在 Arch WSL 内构建。
package-arch:
	$(call wsl_build,./packaging/build.sh arch)

## Windows NSIS 安装包：本机原生构建（GNU 工具链已配置），产物复制到 releases/。
package-windows:
	cd desktop/src-tauri && cargo tauri build --bundles nsis
	@mkdir -p releases && cp desktop/src-tauri/target/release/bundle/nsis/*.exe releases/

## 兼容旧名：本机原生构建 Windows 安装包。
desktop-build-windows: package-windows

## 可选：在 WSL 内通过 Wine 交叉构建 Windows 安装包（需 wine/mingw-w64/rustup gnu）。
package-windows-wine:
	$(call wsl_build,./packaging/build.sh windows-wine)

## 四个发布产物：Windows exe（本机）+ AppImage/deb + Arch pkg（WSL）。
package-all: package-windows package-appimage package-arch

## 配置 WSL Arch 构建环境（需 root）：装工具链、建 builder 用户、装 tauri-cli。
wsl-setup:
	$(WSL) -e bash -lc 'pacman -Syu --noconfirm && pacman -S --noconfirm --needed base-devel rust python git unzip webkit2gtk-4.1 gtk3 libayatana-appindicator librsvg patchelf curl && (useradd -m $(WSL_BUILDER) 2>/dev/null || true) && echo "$(WSL_BUILDER) ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/$(WSL_BUILDER) && chmod 440 /etc/sudoers.d/$(WSL_BUILDER) && su - $(WSL_BUILDER) -c "cargo install tauri-cli --locked --version ^2"'

version:             ## Show the authoritative project version (VERSION)
	./scripts/version.sh show

version-set:         ## Bump the project version everywhere: make version-set VERSION=0.3.0-rc.1
	./scripts/version.sh set "$(VERSION)"

version-check:       ## Verify all first-party version declarations agree (CI-safe, exit 0/1)
	./scripts/version.sh check

check-layers:        ## Dependency-direction gate: application/domain must not import axum/sqlx/reqwest
	./scripts/check-layers.sh
