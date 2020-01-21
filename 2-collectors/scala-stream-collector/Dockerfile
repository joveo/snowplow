FROM snowplow-docker-registry.bintray.io/snowplow/base-debian:0.1.0 as stage0
ADD kinesis/target/docker/stage/opt /opt
ADD config/kinesis.conf /opt/docker
WORKDIR /opt/docker
USER root
RUN ["chmod", "-R", "u=rX,g=rX", "/opt/docker"]
RUN ["chmod", "u+x,g+x", "/opt/docker/bin/snowplow-stream-collector"]

FROM snowplow-docker-registry.bintray.io/snowplow/base-debian:0.1.0
LABEL MAINTAINER="Snowplow Analytics Ltd. <support@snowplowanalytics.com>"
USER root
RUN id -u snowplow 1>/dev/null 2>&1 || (( getent group 0 1>/dev/null 2>&1 || ( type groupadd 1>/dev/null 2>&1 && groupadd -g 0 root || addgroup -g 0 -S root )) && ( type useradd 1>/dev/null 2>&1 && useradd --system --create-home --uid 1001 --gid 0 snowplow || adduser -S -u 1001 -G root snowplow ))
WORKDIR /opt/docker
COPY --from=stage0 --chown=snowplow:root /opt/docker /opt/docker
USER 1001:0
ENTRYPOINT ["/opt/docker/bin/snowplow-stream-collector"]
CMD ["--config", "/opt/docker/kinesis.conf"]
