# Один Dockerfile на все сервисы: docker build --build-arg PROJECT=RefundLedger.Api .
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG PROJECT
WORKDIR /src
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY proto proto
COPY src src
RUN dotnet publish src/${PROJECT} -c Release -o /app -p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG PROJECT
ENV APP=${PROJECT}.dll
WORKDIR /app
COPY --from=build /app .
USER $APP_UID
# exec — чтобы процесс получил SIGTERM и закончил начатое.
ENTRYPOINT ["sh", "-c", "exec dotnet $APP \"$@\"", "--"]
