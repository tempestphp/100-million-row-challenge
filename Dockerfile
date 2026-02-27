FROM php:8.5-cli-alpine

RUN apk add --no-cache \
    $PHPIZE_DEPS \
    linux-headers \
    icu-dev

RUN pecl install igbinary && \
    docker-php-ext-enable igbinary

RUN docker-php-ext-install pcntl shmop sysvmsg sysvsem sysvshm intl

RUN mv "$PHP_INI_DIR/php.ini-production" "$PHP_INI_DIR/php.ini" && \
    sed -i 's/memory_limit = 128M/memory_limit = -1/' "$PHP_INI_DIR/php.ini" && \
    echo "opcache.enable_cli=1" >> "$PHP_INI_DIR/php.ini" && \
    echo "opcache.jit=off" >> "$PHP_INI_DIR/php.ini"

COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

WORKDIR /app

COPY composer.json composer.lock ./

RUN composer install --no-dev --optimize-autoloader

COPY . .

ENTRYPOINT ["php", "tempest"]
