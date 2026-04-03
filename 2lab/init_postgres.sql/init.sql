-- Создаем пользователя
CREATE USER spark WITH PASSWORD 'spark123';

-- Создаем базу данных
CREATE DATABASE etl_lab;

-- Делаем spark владельцем
ALTER DATABASE etl_lab OWNER TO spark;

-- Подключаемся к базе etl_lab
\c etl_lab

-- Даем права
GRANT ALL PRIVILEGES ON SCHEMA public TO spark;