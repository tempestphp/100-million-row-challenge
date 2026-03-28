FROM php:8.5-rc-cli

RUN apt-get update && apt-get install -y git unzip libicu-dev && rm -rf /var/lib/apt/lists/*

# Extensions matching the benchmark server
RUN docker-php-ext-install pcntl intl

# Composer
COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

WORKDIR /app
COPY composer.json composer.lock ./
RUN composer install --prefer-dist --no-interaction --ignore-platform-reqs --no-dev

COPY . .

# Generate data on first run if not mounted
CMD ["php", "tempest", "data:parse"]
