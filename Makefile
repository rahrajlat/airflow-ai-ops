# ----------------------------------
# Config
# ----------------------------------
AIRFLOW_DIR := Airflow_Docker
IMAGE_NAME := airflow_local
IMAGE_TAG := latest
POSTGRESS_DIR := Postgress_Docker

# ----------------------------------
# Phony targets
# ----------------------------------
.PHONY: \
	airflow-build \
	airflow-init \
	airflow-up \
	airflow-down \
	airflow-restart \
	airflow-logs \
	airflow-ps \
	airflow-clean \
	airflow-reset \
	postgress-build \
	postgress-up \
	up

# ----------------------------------
# Start Postgres container
# ----------------------------------
# =========================
# 🚀 ONE COMMAND START
# =========================
up: postgress-build postgress-up airflow-build airflow-init airflow-up


postgress-up:
	docker compose -f $(POSTGRESS_DIR)/docker-compose.yaml up -d



# ----------------------------------
# Build Postgres image
# ----------------------------------
postgress-build:
	docker build $(POSTGRESS_DIR) -t custom-postgres:1.0

# ----------------------------------
# Build Airflow image
# ----------------------------------
airflow-build:
	docker build $(AIRFLOW_DIR) -t $(IMAGE_NAME):$(IMAGE_TAG)

# ----------------------------------
# Initialize Airflow (DB, admin user)
# ----------------------------------
airflow-init:
	cd $(AIRFLOW_DIR) && docker compose up airflow-init

# ----------------------------------
# Start Airflow (detached)
# ----------------------------------
airflow-up:
	cd $(AIRFLOW_DIR) && docker compose up -d

# ----------------------------------
# Stop Airflow containers
# ----------------------------------
airflow-down:
	cd $(AIRFLOW_DIR) && docker compose down

# ----------------------------------
# Restart Airflow
# ----------------------------------
airflow-restart:
	cd $(AIRFLOW_DIR) && docker compose down
	cd $(AIRFLOW_DIR) && docker compose up -d

# ----------------------------------
# Show running containers
# ----------------------------------
airflow-ps:
	cd $(AIRFLOW_DIR) && docker compose ps

# ----------------------------------
# Follow logs
# ----------------------------------
airflow-logs:
	cd $(AIRFLOW_DIR) && docker compose logs -f

# ----------------------------------
# Stop containers + remove volumes
# ----------------------------------
airflow-clean:
	cd $(AIRFLOW_DIR) && docker compose down -v

# ----------------------------------
# Full reset (containers + volumes + image)
# ----------------------------------
airflow-reset: airflow-clean
	docker rmi $(IMAGE_NAME):$(IMAGE_TAG) || true