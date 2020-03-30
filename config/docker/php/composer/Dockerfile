FROM php:7.4-cli

RUN apt-get update && apt-get install -y libonig-dev zip \
    && docker-php-ext-install mbstring

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir /usr/local/bin --filename composer

ENTRYPOINT ["/usr/local/bin/composer"]

WORKDIR /scripts
