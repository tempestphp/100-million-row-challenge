FROM php:8.5-rc-cli

# Install build dependencies, extensions via PECL and apt
RUN apt-get update && apt-get install -y \
    $PHPIZE_DEPS \
    libicu-dev \
    && pecl install igbinary \
    && docker-php-ext-enable igbinary \
    && docker-php-ext-install pcntl intl shmop sysvshm sysvsem sysvmsg \
    && apt-get purge -y $PHPIZE_DEPS \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Set memory limit to match container constraint (1.5GB container)
RUN echo "memory_limit=1500M" > /usr/local/etc/php/conf.d/memory.ini

# Copy application files (data is mounted via volume)
COPY composer.json composer.lock ./
COPY app ./app
COPY tempest ./
COPY vendor ./vendor

# Default command
CMD ["php", "tempest", "data:parse"]
