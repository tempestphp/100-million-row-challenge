FROM php:8.5-cli

RUN apt-get update && apt-get install -y libicu-dev zlib1g-dev git && rm -rf /var/lib/apt/lists/* \
    && docker-php-ext-install intl pcntl shmop \
    && git clone --branch release/latest --depth 1 https://github.com/NoiseByNorthwest/php-spx.git /tmp/php-spx \
    && cd /tmp/php-spx \
    && phpize && ./configure && make && make install \
    && docker-php-ext-enable spx \
    && rm -rf /tmp/php-spx

RUN git config --global --add safe.directory /app

WORKDIR /app
