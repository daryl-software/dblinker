FROM php:7.1-fpm-alpine

RUN docker-php-ext-install mbstring

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir /usr/local/bin --filename composer

ENTRYPOINT ["/usr/local/bin/composer"]

WORKDIR /scripts
