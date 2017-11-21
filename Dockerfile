FROM node:7.9.0-alpine
MAINTAINER libo@yodamob.com  
ENV LOGTYPE request
#EXPOSE 3000
RUN mkdir -p /home/etl
WORKDIR /home/etl
ADD . /home/etl
RUN npm install
RUN node -v
CMD node /home/etl/main.js -l $LOGTYPE
