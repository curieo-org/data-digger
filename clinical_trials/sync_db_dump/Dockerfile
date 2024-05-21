FROM alpine:latest

RUN apk update && apk upgrade && apk add wget && apk add postgresql && apk add --no-cache bash

WORKDIR /usr/local/bin

COPY sync_db_dump.sh /usr/local/bin/script.sh
COPY queries.sql /usr/local/bin/queries.sql

RUN chmod +x /usr/local/bin/script.sh

CMD ["bash", "/usr/local/bin/script.sh"]