SHELL = /bin/bash
BASE_URL := 777708544714.dkr.ecr.us-west-2.amazonaws.com
IMAGE_jammy-amd64 := --platform linux/amd64 $(BASE_URL)/openpathsec/ubuntu-builder:jammy-amd64

NAME := op-clustershell

define package_version
$(shell python3 setup.py --version)$(if $(strip $(EXTRA_VERSION)),$(or $(1),~)$(shell echo $${EXTRA_VERSION} | sed -r 's/\/|_/-/g'))
endef

define git_author
$(shell git log --format="-- %an <%ae>  %ad" --date=rfc -1)
endef

define create_changelog
$(shell git log --format=" * %s\\n" origin/master..HEAD | tr -d "'\#" | sed -E 's/[ \t]+$$//')
endef

ifndef OPIAN_BUILDER

.make-ecr-login:
	@aws ecr get-login-password --profile dev | docker login --username AWS --password-stdin $(BASE_URL)
	docker pull $(IMAGE_jammy-amd64)
	@touch $@

run-%: .make-ecr-login
	docker run -it --rm -e EXTRA_VERSION=${EXTRA_VERSION} -v "${CURDIR}":"/build/src" $(IMAGE_$*)

.PHONY: run
run: run-buster-arm64

test-%: .make-ecr-login
	docker run -it --rm -e EXTRA_VERSION=${EXTRA_VERSION} -v "${CURDIR}":"/build/src" $(IMAGE_$*) make test

.PHONY: test
test: test-buster-arm64

deb-%: .make-ecr-login
	docker run -it --init --rm -e EXTRA_VERSION=${EXTRA_VERSION} -v "${CURDIR}":"/build/src" $(IMAGE_$*) make

.PHONY: deb-all
deb-all: deb-jammy-amd64

# TODO: this is ugly. would be nice if opdeploy knew how to parse the filename itself so we don't need to specify --arch --dist
deploy-%:
	-opdeploy deb --env $* --arch amd64 --dist jammy dist/jammy-amd64/$(NAME)_$(call package_version)_amd64.deb
	-opdeploy deb --env $* --arch all --dist jammy dist/jammy-amd64/$(NAME)-tools_$(call package_version)_all.deb

.PHONY: deploy
deploy: deploy-sandbox

.PHONY: version
version:
	@echo $(call package_version)

git-tag:
	@if [ $$(git rev-list $$(git describe --abbrev=0 --tags)..HEAD --count) -gt 0 ]; then\
		if $$(git diff --quiet); then \
			if [ $$(git log @{u}.. --oneline | wc -l) -eq 0 ]; then \
				git tag $(call package_version,-) && git push origin $(call package_version,-) || echo "Version already released, update your version!"; \
			else\
				echo "There are unpushed commits!";\
				exit 1;\
			fi; \
		else\
			echo "There are uncommitted changes!";\
			exit 1;\
		fi; \
	else\
		echo "No commits since last release!";\
		exit 1;\
	fi


.PHONY: changelog
changelog:
	@sed -i "" '1s#^#${NAME} ($(call package_version)) unstable; urgency=medium\n\n $(call create_changelog)\n $(call git_author)\n\n#' debian/changelog
	git commit -a --fixup HEAD; GIT_SEQUENCE_EDITOR=: git rebase -i --autosquash HEAD~2

.PHONY: clean
clean:
	@rm -f .make-docker-image*
	@rm -f .make-ecr-login
	@rm -f test/__pycache__


endif

# This is run from within the Docker container.
# put the commands to build the package under the "all" task
ifdef OPIAN_BUILDER

include /usr/local/include/opian_builder.env

BUILD_SRC_COPY = /build/tmp
OPIAN_DIST_ARCH = $(OPIAN_DIST)-$(OPIAN_ARCH)

.DEFAULT_GOAL := all
.PHONY: all default

$(BUILD_SRC_COPY)/Makefile:
	cp -pur $(CURDIR) $(BUILD_SRC_COPY)/src
# This resolves issues where the files have executable bit set, but are not actually executable according to the "file" command
# This mainly solves issues in WSL where file permissions might not be set correctly.
	find $(BUILD_SRC_COPY)/src/debian -type f -exec sh -c 'file "{}" | grep -qv "executable$$" && chmod a-x "{}"' \;

all: $(BUILD_SRC_COPY)/Makefile
	$(MAKE) -C ${BUILD_SRC_COPY}/src default
	mkdir -p $(CURDIR)/dist/$(OPIAN_DIST_ARCH)/
	cp -pur ${BUILD_SRC_COPY}/*.deb $(CURDIR)/dist/$(OPIAN_DIST_ARCH)/
	$(if $(strip $(GITHUB_OUTPUT)), @echo "package-version=$(call package_version)" >> ${GITHUB_OUTPUT}, @echo "package-version=$(call package_version)")
	$(if $(strip $(GITHUB_OUTPUT)), @echo "package-name=$(NAME)_$(call package_version)_$(shell dpkg --print-architecture)" >> ${GITHUB_OUTPUT}, @echo "package-name=$(NAME)_$(call package_version)_$(shell dpkg --print-architecture)")

test:
	python3 -m pip install -r requirements.txt -r requirements_test.txt
	python3 -m pytest --md-report --md-report-flavor gfm --md-report-output test-results.md
	$(if $(strip $(GITHUB_OUTPUT)), cat test-results.md >> ${GITHUB_STEP_SUMMARY})

default:
	DPKG_GENCONTROL_ARGS="-v$(call package_version)" dpkg-buildpackage -d -us -uc -b -tc

endif
