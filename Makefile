.DEFAULT_GOAL := setup

DOCKER ?= docker
UV ?= uv
FORCE ?= 0
IMAGE ?= befly-pipeline
INPUT_DIR ?= data/input
OUTPUT_DIR ?= data/output
MASTER ?= local[2]

.PHONY: setup download run unittest

setup: download
	$(DOCKER) build -t "$(IMAGE)" .

download:
	@if [ -s "$(INPUT_DIR)/hotel_bookings.csv" ] && [ "$(FORCE)" != "1" ]; then \
		printf 'Using existing %s/hotel_bookings.csv\n' "$(INPUT_DIR)"; \
	else \
		$(UV) run --no-project --with kagglehub==0.3.13 \
			python scripts/download.py --input-dir "$(INPUT_DIR)" \
			$(if $(filter 1,$(FORCE)),--force,); \
	fi

run: setup
	mkdir -p "$(OUTPUT_DIR)"
	$(DOCKER) run --rm \
		-v "$$(realpath "$(INPUT_DIR)"):/input:ro" \
		-v "$$(realpath "$(OUTPUT_DIR)"):/output" \
		"$(IMAGE)" python -m pipeline \
		--input-dir /input --output-dir /output --master "$(MASTER)"

unittest: setup
	$(DOCKER) run --rm \
		-v "$$(realpath "$(INPUT_DIR)"):/app/data/input:ro" \
		"$(IMAGE)" python -m unittest discover -s tests -v
