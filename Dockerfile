FROM ubuntu:latest
LABEL authors="shusant.sapkota"

ENTRYPOINT ["top", "-b"]