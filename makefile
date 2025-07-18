# Makefile for managing Docker Compose services
# Assumes docker-compose.yml and .env are in the project root
# Services: mysql, airflow-init, airflow-webserver, airflow-scheduler

# Variables
COMPOSE_FILE := docker-compose.yml
SERVICES := mysql airflow-init airflow-webserver airflow-scheduler
COMPOSE := docker-compose -f $(COMPOSE_FILE)

# Default target
.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make build-<service>         - Build individual service (e.g., build-mysql)"
	@echo "  make up-<service>           - Start individual service in foreground (e.g., up-mysql)"
	@echo "  make build              - Build all services"
	@echo "  make clean-volumes          - Remove all volumes"
	@echo "  make clean              - Remove all containers, images, volumes, and networks"
	@echo "  make clean-<service>        - Remove individual service container (e.g., clean-mysql)"
	@echo "  make up-<service>-detached  - Start individual service in detached mode (e.g., up-mysql-detached)"
	@echo "  make up                 - Start all services in detached mode"
	@echo "  make status                 - Check running services"
	@echo "  make logs-<service>         - View logs for a service (e.g., logs-mysql)"
	@echo "  make start              - Start all stopped containers"
	@echo "  make stop              - Stop all running containers"
	@echo "  make start-<service>        - Start a stopped service (e.g., start-mysql)"
	@echo "  make stop-<service>         - Stop a running service (e.g., stop-mysql)"

# 1. Build individual services
.PHONY: $(addprefix build-,$(SERVICES))
$(addprefix build-,$(SERVICES)):
	$(COMPOSE) build $(subst build-,,$(@))

# 2. Build all services
.PHONY: build
build:
	$(COMPOSE) build

# 3. Clean up volumes
.PHONY: clean-volumes
clean-volumes:
	$(COMPOSE) down -v
	@echo "All volumes removed"

# 4. Clean up everything (containers, images, volumes, networks)
.PHONY: clean
clean:
	$(COMPOSE) down --rmi all -v --remove-orphans
	@echo "All containers, images, volumes, and networks removed"

# 5. Clean individual service containers
.PHONY: $(addprefix clean-,$(SERVICES))
$(addprefix clean-,$(SERVICES)):
	$(COMPOSE) rm -f $(subst clean-,,$(@))
	@echo "Container for $(subst clean-,,$(@)) removed"

# 6 & 7. Start individual services (foreground and detached)
.PHONY: $(addprefix up-,$(SERVICES))
$(addprefix up-,$(SERVICES)):
	$(COMPOSE) up $(subst up-,,$(@))

.PHONY: $(addprefix up-,$(addsuffix -detached,$(SERVICES)))
$(addprefix up-,$(addsuffix -detached,$(SERVICES))):
	$(COMPOSE) up -d $(subst up-,,$(subst -detached,,$(@)))

# 8. Check running services
.PHONY: ps
ps:
	$(COMPOSE) ps

# 9. Check logs for each service
.PHONY: $(addprefix logs-,$(SERVICES))
$(addprefix logs-,$(SERVICES)):
	$(COMPOSE) logs --tail=100 -f $(subst logs-,,$(@))

# 10. Start/stop all services
.PHONY: start
start:
	$(COMPOSE) start
	@echo "All services started"

.PHONY: stop
stop:
	$(COMPOSE) stop
	@echo "All services stopped"

# 11. Start/stop individual services
.PHONY: $(addprefix start-,$(SERVICES))
$(addprefix start-,$(SERVICES)):
	$(COMPOSE) start $(subst start-,,$(@))
	@echo "$(subst start-,,$(@)) started"

.PHONY: $(addprefix stop-,$(SERVICES))
$(addprefix stop-,$(SERVICES)):
	$(COMPOSE) stop $(subst stop-,,$(@))
	@echo "$(subst stop-,,$(@)) stopped"

# 12. Start all services in detached mode
.PHONY: up
up:
	$(COMPOSE) up -d $(SERVICES)
	@echo "All services started in detached mode"

# 13. Stop and remove all services
.PHONY: down
down:
	$(COMPOSE) down
	@echo "All services stopped and removed"

.PHONY: downv
downv:
	$(COMPOSE) down -v --remove-orphans
	@echo "All services stopped, volumes removed, orphans cleaned"

# Usage Instructions
# 1. Ensure docker-compose.yml and .env are in the project root.
# 2. Ensure .env contains required variables:
#    MYSQL_ROOT_PASSWORD, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD,
#    AIRFLOW_WEBSERVER_SECRET_KEY, AIRFLOW_WWW_USER_USERNAME, AIRFLOW_WWW_USER_PASSWORD
# 3. Ensure ./airflow/Dockerfile includes:
#    FROM apache/airflow:2.9.3
#    RUN pip install apache-airflow-providers-mysql>=3.2.0 mysqlclient>=2.1.0
# 4. Ensure directories exist: ./mysql, ./airflow/dags, ./airflow/etl_scripts, ./logs
# 5. Run commands, e.g.:
#    make build-all                # Build all services
#    make up-all                   # Start all services in detached mode
#    make logs-airflow-webserver   # View airflow-webserver logs
#    make status                   # Check running services
#    make stop-mysql               # Stop mysql service
#    make clean-all                # Remove all resources (use with caution)
# 6. Access Airflow at http://localhost:8080 (or port set in AIRFLOW_WEBSERVER_PORT)
#    Login with AIRFLOW_WWW_USER_USERNAME and AIRFLOW_WWW_USER_pASSWORD from .env
# 7. Note: Run make up-airflow-init-detached before airflow-webserver or airflow-scheduler
#    due to dependency requirements.