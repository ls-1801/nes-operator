FROM alpine:latest
RUN apk update && apk add curl jq
COPY entrypoint.sh /entrypoint.sh
CMD [ "/bin/sh", "/entrypoint.sh" ]
