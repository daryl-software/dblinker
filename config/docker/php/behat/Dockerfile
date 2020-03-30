FROM php:7.4-cli

RUN apt-get update && apt-get install -y libpq-dev \
    && docker-php-ext-configure pgsql \
    && docker-php-ext-install -j$(nproc) pcntl pdo_pgsql pdo_mysql mysqli \
    && docker-php-ext-enable pcntl pdo_pgsql pdo_mysql mysqli

RUN pecl install redis apcu xdebug \
    && docker-php-ext-enable redis xdebug apcu

COPY apc.ini /etc/php/7.4/mods-available/apcu.ini

ENTRYPOINT ["php", "/scripts/vendor/bin/behat"]

EXPOSE 9000

WORKDIR /scripts
