FROM php:5.6

RUN apt-get update
RUN apt-get install -y zlib1g-dev
RUN docker-php-ext-install zip

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir /usr/local/bin --filename composer

RUN docker-php-ext-install mbstring

ENTRYPOINT ["/usr/local/bin/composer"]

WORKDIR /scripts
