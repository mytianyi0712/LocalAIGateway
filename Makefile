.PHONY: install build test run desktop-icons desktop-build-linux desktop-build-windows package-appimage package-arch package-windows-wine package-all

install:
	cd backend && uv sync --extra test

build:
	@echo "Native frontend: no build step required"

test:
	cd backend && uv run pytest -q

run:
	cd backend && uv run alembic upgrade head
	cd backend && uv run python -m app

desktop-icons:
	cd desktop/src-tauri && cargo tauri icon icons/icon.png

desktop-build-linux:
	./desktop/scripts/build-appimage.sh

desktop-build-windows:
	cd desktop/src-tauri && cargo tauri build --bundles nsis

package-appimage:
	./packaging/build.sh appimage

package-arch:
	./packaging/build.sh arch

package-windows-wine:
	./packaging/build.sh windows-wine

package-all:
	./packaging/build.sh all
