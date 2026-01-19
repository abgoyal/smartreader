# HN New - Build and deployment helpers

.PHONY: ui.zip run run-public clean release lint format check

# Build ui.zip from frontend directory
ui.zip: frontend/index.html frontend/app.js frontend/style.css frontend/manifest.json frontend/icon.svg frontend/sw.js
	cd frontend && zip -r ../ui.zip index.html app.js style.css manifest.json icon.svg sw.js
	@echo "Created ui.zip"

# Run locally (no auth)
run:
	./hn_new.py

# Run with auth (for public deployment)
run-public:
	@if [ -z "$$HN_USER" ] || [ -z "$$HN_PASSWORD" ]; then \
		echo "Error: Set HN_USER and HN_PASSWORD environment variables"; \
		exit 1; \
	fi
	./hn_new.py --public

# Build release archive (for GitHub releases)
release: ui.zip
	mkdir -p _release
	cp hn_new.py fetch_content.py ui.zip .env.example README.md Caddyfile hn-new.service _release/
	cd _release && zip -r ../hn-new-release.zip .
	rm -rf _release
	@echo "Created hn-new-release.zip"

# Deploy: copy just the necessary files
deploy: ui.zip
	@echo "Files to deploy:"
	@echo "  - hn_new.py"
	@echo "  - ui.zip"
	@echo "  - .env (create from .env.example)"
	@echo ""
	@echo "On server, run:"
	@echo "  ./hn_new.py --public"

# Clean build artifacts
clean:
	rm -f ui.zip hn-new-release.zip
	rm -rf _release

# Lint Python code (check only)
lint:
	uvx ruff check hn_new.py fetch_content.py

# Format Python code
format:
	uvx ruff format hn_new.py fetch_content.py
	uvx ruff check --fix hn_new.py fetch_content.py

# Run all checks (for CI)
check: lint
	uvx ruff format --check hn_new.py fetch_content.py
