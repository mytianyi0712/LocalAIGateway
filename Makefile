# Local AI Gateway 构建入口。
#
# 本机（Windows 11 + MSYS make + rustup x86_64-pc-windows-gnu）：
#   1. 只改仓库根目录 VERSION
#   2. make package-all
#
# 产物写入 releases/：
#   - Local AI Gateway_<ver>_x64-setup.exe      本机 cargo tauri (NSIS)
#   - Local AI Gateway_<ver>_amd64.AppImage     Arch WSL
#   - local-ai-gateway_<ver>_amd64.deb          Arch WSL
#   - local-ai-gateway-<pkgver>-1-x86_64.pkg.tar.zst  Arch WSL
#
# Linux 宿主：AppImage/deb/Arch 本机构建，Windows 走 Wine 交叉编译。
# 首次配置 WSL：make wsl-setup

.DEFAULT_GOAL := help

.PHONY: help doctor desktop-icons \
	package-windows package-appimage package-arch package-windows-wine package-all \
	version version-sync version-set version-check \
	check-layers wsl-setup \
	desktop-build-linux desktop-build-windows

# Recipes are POSIX. MSYS make already uses /bin/sh; cmd.exe make gets bash.
ifeq ($(OS),Windows_NT)
HOST := windows
ifneq ($(findstring cmd,$(SHELL)),)
SHELL := bash
.SHELLFLAGS := -c
endif
# Mixed Windows path (D:/...) for wsl --cd and human logs.
ifneq ($(shell command -v cygpath 2>/dev/null),)
WIN_ROOT := $(shell cygpath -m "$(CURDIR)")
else
WIN_ROOT := $(subst \,/,$(CURDIR))
endif
WSL_DRIVE := $(shell printf '%s' '$(WIN_ROOT)' | cut -c1 | tr '[:upper:]' '[:lower:]')
WSL_ROOT := /mnt/$(WSL_DRIVE)$(shell printf '%s' '$(WIN_ROOT)' | cut -c3-)
else
HOST := unix
WIN_ROOT := $(CURDIR)
WSL_ROOT := $(CURDIR)
endif

WSL ?= wsl
WSL_DISTRO ?= archlinux
WSL_BUILDER ?= builder
BASH ?= bash
VERSION_SH = $(BASH) scripts/version.sh
BUILD_SH = $(BASH) packaging/build.sh

# $1 is a bash command run at the repo root.
# Windows: Arch WSL as non-root builder (makepkg refuses root).
# Cargo artifacts go to $HOME/.cache so they never share target/ with the
# Windows host (same `release/` name, different ABI) and so chmod/exec work
# — drvfs on /mnt/d cannot set the execute bit.
# Unix: the same command on the host.
ifeq ($(HOST),windows)
define linux_sh
	$(WSL) -d $(WSL_DISTRO) -u $(WSL_BUILDER) -e bash -lc 'export CARGO_TARGET_DIR="$$HOME/.cache/local-ai-gateway/target"; mkdir -p "$$CARGO_TARGET_DIR"; cd "$(WSL_ROOT)" && $(1)'
endef
else
define linux_sh
	$(1)
endef
endif

help: ## 列出常用目标
	@printf '%s\n' \
		'用法: make <target>' \
		'' \
		'发布（先改 VERSION，再一条命令出齐全部安装包）:' \
		'  make package-all          同步版本 + Windows NSIS + AppImage/deb + Arch pkg' \
		'' \
		'版本（权威源是仓库根目录 VERSION）:' \
		'  make version              显示 VERSION' \
		'  make version-sync         把 VERSION 同步到 Cargo.toml / tauri.conf.json / Cargo.lock / PKGBUILD' \
		'  make version-set VERSION=0.3.0   写入 VERSION 并同步（可选）' \
		'  make version-check        校验所有第一方声明与 VERSION 一致' \
		'' \
		'单包:' \
		'  make package-windows      本机 NSIS exe' \
		'  make package-appimage     Arch WSL: .deb + AppImage' \
		'  make package-arch         Arch WSL: .pkg.tar.zst' \
		'  make package-windows-wine WSL Wine 交叉编译 NSIS（可选）' \
		'' \
		'环境:' \
		'  make doctor               打印宿主 / WSL 路径与工具' \
		'  make wsl-setup            配置 Arch WSL 构建用户与依赖' \
		'  make check-layers         依赖方向门禁' \
		'  make desktop-icons        从 SVG 重生托盘图标'

doctor: ## 打印构建宿主与 WSL 路径，不启动编译
	@printf 'host=%s\nCURDIR=%s\nWIN_ROOT=%s\nWSL_ROOT=%s\nSHELL=%s\n' \
		'$(HOST)' '$(CURDIR)' '$(WIN_ROOT)' '$(WSL_ROOT)' '$(SHELL)'
	@command -v cargo >/dev/null && cargo --version || echo 'cargo: missing'
	@command -v rustc >/dev/null && rustc --version || echo 'rustc: missing'
	@cargo tauri --version 2>/dev/null || echo 'tauri-cli: missing'
ifeq ($(HOST),windows)
	@$(WSL) -d $(WSL_DISTRO) -u $(WSL_BUILDER) -e bash -lc 'cd "$(WSL_ROOT)" && echo wsl_user=$$(whoami) && echo wsl_pwd=$$(pwd) && command -v cargo && command -v makepkg && cargo tauri --version'
endif

desktop-icons: ## 从 icons/icon.svg 生成桌面图标
	cd desktop/src-tauri && cargo tauri icon icons/icon.svg

package-windows: version-sync ## 本机构建 NSIS 安装包并收集到 releases/
ifneq ($(HOST),windows)
	$(error package-windows 需要 Windows 宿主；Linux 请用 make package-windows-wine)
endif
	$(BUILD_SH) windows

package-appimage: version-sync ## 构建 .deb + AppImage（Windows 上经 Arch WSL）
	$(call linux_sh,$(BUILD_SH) appimage)

package-arch: version-sync ## 构建 Arch .pkg.tar.zst（Windows 上经 Arch WSL）
	$(call linux_sh,$(BUILD_SH) arch)

package-windows-wine: version-sync ## 在 WSL/Linux 上经 Wine 交叉构建 NSIS
	$(call linux_sh,$(BUILD_SH) windows-wine)

# Windows 宿主：原生 exe + WSL Linux 三件套。
# Unix 宿主：本机 Linux 包 + Wine NSIS。
ifeq ($(HOST),windows)
package-all: version-sync package-windows package-appimage package-arch ## 构建当前宿主能产出的全部 release
else
package-all: version-sync package-appimage package-arch package-windows-wine ## 构建当前宿主能产出的全部 release
endif

desktop-build-linux: package-appimage
desktop-build-windows: package-windows

wsl-setup: ## 配置 Arch WSL：工具链、builder 用户、tauri-cli
ifneq ($(HOST),windows)
	$(error wsl-setup 只在 Windows 宿主上有意义)
endif
	$(WSL) -d $(WSL_DISTRO) -e bash -lc 'set -euo pipefail; \
		pacman -Syu --noconfirm; \
		pacman -S --noconfirm --needed base-devel rust python git unzip curl \
			webkit2gtk-4.1 gtk3 libayatana-appindicator librsvg patchelf; \
		id $(WSL_BUILDER) >/dev/null 2>&1 || useradd -m $(WSL_BUILDER); \
		echo "$(WSL_BUILDER) ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/$(WSL_BUILDER); \
		chmod 440 /etc/sudoers.d/$(WSL_BUILDER); \
		su - $(WSL_BUILDER) -c "command -v cargo >/dev/null && cargo install tauri-cli --locked --version ^2"'

version: ## 显示权威版本（VERSION）
	$(VERSION_SH) show

version-sync: ## 以 VERSION 为准同步所有第一方版本声明
	$(VERSION_SH) sync

version-set: ## 写入 VERSION 并同步：make version-set VERSION=0.3.0-rc.1
	$(VERSION_SH) set "$(VERSION)"

version-check: ## 校验全部第一方版本声明与 VERSION 一致
	$(VERSION_SH) check

check-layers: ## 依赖方向门禁
	$(BASH) scripts/check-layers.sh
