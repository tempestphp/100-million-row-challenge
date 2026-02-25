FROM php:8.5-cli

RUN apt-get update && apt-get install -y \
    libzip-dev \
    libicu-dev \
    unzip \
    git \
    && docker-php-ext-install pcntl shmop zip intl \
    && pecl install igbinary && docker-php-ext-enable igbinary \
    && rm -rf /var/lib/apt/lists/*

COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

RUN echo "memory_limit=1536M" > /usr/local/etc/php/conf.d/memory.ini

WORKDIR /app
