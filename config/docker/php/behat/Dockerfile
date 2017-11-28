FROM php:7.1

RUN apt-get update
RUN apt-get install -y git file libpq-dev
RUN cd /tmp/ && git clone git://github.com/xdebug/xdebug.git
RUN cd /tmp/xdebug && git checkout XDEBUG_2_5_5 && phpize && ./configure --enable-xdebug && make && cp modules/xdebug.so /usr/local/lib/php/extensions/

RUN docker-php-ext-install mbstring
RUN docker-php-ext-install pdo_mysql pdo_pgsql mysqli
RUN docker-php-ext-install pcntl

ENTRYPOINT ["php", "/scripts/vendor/bin/behat"]

EXPOSE 9000

WORKDIR /scripts
