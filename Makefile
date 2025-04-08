NETWORK = oapd

.PHONY: ps stop rm prune network build compose

# Отображение всех контейнеров
ps:
	docker ps -a
# Остановка всех запущенных контейнеров
stop:
	@docker ps -q | xargs -r docker stop || true
# Удаление всех контейнеров
rm:
	@docker ps -a -q | xargs -r docker rm || true
# Очистка контейнеров, образов и томов без удаления сетей
prune:
	@docker container prune -f
	@docker image prune -af
	@docker volume prune -f
# Создание сети если она не существует
network:
	@if ! docker network ls --filter name=^$(NETWORK)$$ --format "{{.Name}}" | grep -w $(NETWORK) > /dev/null; then \
  docker network create $(NETWORK); fi
# Пересобрать контейнер
build: stop rm prune network compose

	#make network
	#docker build . --tag fastapi && docker run -d -p 80:80 --name fastapi --network oapd fastapi
	#docker build /home/docx_gen --tag docx_gen && docker run -d -p 81:81 --network oapd docx_gen

pull:
	git pull origin dev

test:
	pytest --cache-clear

lint:
	flake8 db_manager request_models response_models routes tests

killproc:
	netstat -ano | findstr :8000
	taskkill /F /PID ...

compose:
	docker compose up -d

git_init:
	apt-get install -y git
	git config --global user.name "krasnopolskiyia"
	git config --global user.email "krasnopolskiyia@eipp.ru"
	git clone http://krasnopolskiyia:Qawse321@gitlab.grp.local/spatial/db_manager.git || { echo 'Clone failed'; exit 1; }
	cd db_manager && git checkout main || { echo 'Checkout failed'; exit 1; }
	cd db_manager && git pull origin main || { echo 'Pull failed'; exit 1; }
	cd db_manager && chmod +x git_pull.sh
