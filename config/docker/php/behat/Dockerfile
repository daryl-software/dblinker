FROM php:7.1

RUN apt-get update
RUN apt-get install -y git file libpq-dev php-apcu php7.1-dev php7.1-mbstring php7.1-mysql php7.1-pgsql
RUN cd /tmp/ && git clone git://github.com/xdebug/xdebug.git
RUN cd /tmp/xdebug && git checkout XDEBUG_2_5_5 && phpize && ./configure --enable-xdebug && make && make install

COPY apc.ini /etc/php/7.1/mods-available/apcu.ini
RUN phpenmod -v ALL -s ALL apcu


ENTRYPOINT ["php", "/scripts/vendor/bin/behat"]

EXPOSE 9000

WORKDIR /scripts
