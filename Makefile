.PHONY: desktop-icons desktop-build-linux desktop-build-windows package-appimage package-arch package-windows-wine package-all version version-set version-check

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

version:             ## Show the authoritative project version (VERSION)
	./scripts/version.sh show

version-set:         ## Bump the project version everywhere: make version-set VERSION=0.3.0-rc.1
	./scripts/version.sh set "$(VERSION)"

version-check:       ## Verify all first-party version declarations agree (CI-safe, exit 0/1)
	./scripts/version.sh check

check-layers:        ## Dependency-direction gate: application/domain must not import axum/sqlx/reqwest
	./scripts/check-layers.sh
