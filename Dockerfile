FROM php:8.5-cli-alpine

# Install dependencies required by composer and some PHP extensions
RUN apk add --no-cache git unzip zip icu-dev \
    && docker-php-ext-install bcmath intl \
    && git config --global --add safe.directory /app

# Copy composer from official image
COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

# Set working directory
WORKDIR /app
