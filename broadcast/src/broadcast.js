#!/usr/bin/env node

require('@babel/register')({ extensions: ['.js', '.ts'] });

require('dotenv').config();

const { TelegramClient } = require('messaging-api-telegram');
const subDays = require('date-fns/subDays');
const format = require('date-fns/format');
const pMap = require('p-map');

const getDatabase = require('./database');

const botToken = process.env.BOT_TOKEN;
const userId = process.env.USER_ID;

const bot = new TelegramClient({
  accessToken: botToken,
});

const main = async () => {
  const db = await getDatabase();

  const today = new Date();
  const todayDate = new Date(
    Date.UTC(today.getFullYear(), today.getMonth(), today.getDate())
  );

  const dailyMessages = await db
    .collection('company_daily_messages')
    .find({
      date: { $gte: subDays(todayDate, 3) },
      title: { $regex: '(?=.*處分)(?!.*存款)(?!.*理財).*' },
    })
    .toArray();

  const messages = dailyMessages.map((msg) => {
    const title =
      `title: ${msg.title}\n` +
      `date: ${format(msg.date, 'yyyy-MM-dd')}\n` +
      `time: ${msg.time}`;

    return {
      title,
      option: {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [
              {
                text: `${msg.company_name} ${msg.company_code}, ${msg.typek}`,
                url: msg.url,
              },
            ],
          ],
        },
      },
    };
  });

  await pMap(
    messages,
    async (message) => {
      await bot.sendMessage(userId, message.title, message.option);
    },
    { concurrency: 1 }
  );
};

main()
  .catch(console.error)
  .then(() => process.exit(0));
