FROM php:8.5-cli-alpine

RUN apk add --no-cache git unzip curl icu-dev bash \
    && docker-php-ext-install intl pcntl \
    && curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer

WORKDIR /app

COPY composer.json ./

RUN composer install --no-dev --no-scripts --no-autoloader --prefer-dist

COPY . .

RUN composer dump-autoload --optimize

CMD ["tail", "-f", "/dev/null"]
