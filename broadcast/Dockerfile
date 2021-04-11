FROM node:12

WORKDIR /home/app

COPY ./src ./src
COPY ./package.json ./package.json
COPY ./yarn.lock ./yarn.lock
COPY ./index.js ./index.js

RUN yarn

EXPOSE 5000

CMD ["yarn", "start"]

