FROM alpine:latest

RUN apk update && apk upgrade && apk add wget && apk add aws-cli && apk add postgresql && apk add --no-cache bash

COPY update-fulltext-xmls.sh /usr/local/bin/script.sh

RUN chmod +x /usr/local/bin/script.sh

CMD ["bash", "/usr/local/bin/script.sh"]
