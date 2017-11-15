FROM node:7.9.0-alpine
MAINTAINER libo@yodamob.com  
ENV LOGTYPE request
#EXPOSE 3000
# EXPOSE 8181
RUN mkdir -p /home/etl
WORKDIR /home/etl
ADD . /home/etl
RUN npm install
RUN node -v
#CMD ["node" 'main.js','-l','request']
CMD node /home/tracketl/main.js -l $LOGTYPE
# CMD node /home/tracketl/index.js