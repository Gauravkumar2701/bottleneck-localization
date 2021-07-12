WORKDIR /app
# copy binary into image
COPY main /app/
ENTRYPOINT ["./main"]
EXPOSE 3000