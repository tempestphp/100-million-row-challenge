FROM php:8.5-rc-cli

RUN apt-get update && apt-get install -y \
    git \
    unzip \
    libicu-dev \
    libgmp-dev \
    libzip-dev \
    && docker-php-ext-install \
    pcntl \
    shmop \
    sysvsem \
    sysvshm \
    sysvmsg \
    intl \
    gmp \
    zip \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN echo "opcache.enable=1\n\
opcache.enable_cli=1\n\
opcache.jit=disable\n\
opcache.jit_buffer_size=0\n\
opcache.memory_consumption=256\n\
opcache.interned_strings_buffer=16\n\
opcache.max_accelerated_files=20000" > /usr/local/etc/php/conf.d/opcache.ini

COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

WORKDIR /app

COPY composer.json composer.lock ./
RUN composer install --no-dev --optimize-autoloader --no-interaction

COPY . .

CMD ["php", "tempest", "data:parse"]
