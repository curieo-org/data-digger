FROM alpine:latest

COPY update-fulltext-xmls.sh /usr/local/bin/script.sh

RUN chmod +x /usr/local/bin/script.sh

CMD ["sh", "-c", "/usr/local/bin/script.sh"]
