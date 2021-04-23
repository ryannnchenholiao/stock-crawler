FROM node:12

WORKDIR /home/app

COPY package.json yarn.lock ./

RUN yarn

COPY . .

CMD ["yarn", "broadcast"]

