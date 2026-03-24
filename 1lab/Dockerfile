FROM postgres

ENV POSTGRES_HOST_AUTH_METHOD=trust
#ENV POSTGRES_PASSWORD=password

WORKDIR /data

VOLUME ["/data"]

CMD [ "./script.sh" ]