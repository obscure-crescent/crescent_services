FROM mcr.microsoft.com/dotnet/sdk:9.0 as BUILD

COPY MareAPI /server/MareAPI
COPY MareSynchronosServer/MareSynchronosShared /server/MareSynchronosServer/MareSynchronosShared
COPY MareSynchronosServer/MareSynchronosServices /server/MareSynchronosServer/MareSynchronosServices

WORKDIR /server/MareSynchronosServer/MareSynchronosServices/

RUN dotnet publish \
        --configuration=Debug \
        --os=linux \
        --output=/build \
        MareSynchronosServices.csproj

FROM mcr.microsoft.com/dotnet/aspnet:9.0

RUN adduser \
        --disabled-password \
        --group \
        --no-create-home \
        --quiet \
        --system \
        mare

COPY --from=BUILD /build /opt/MareSynchronosServices
RUN chown -R mare:mare /opt/MareSynchronosServices
RUN apt-get update; apt-get install curl -y

USER mare:mare
WORKDIR /opt/MareSynchronosServices

CMD ["./MareSynchronosServices"]
