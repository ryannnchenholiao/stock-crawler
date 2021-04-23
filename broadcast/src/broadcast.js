#!/usr/bin/env node

require('@babel/register')({ extensions: ['.js', '.ts'] });

require('dotenv').config();

const { TelegramClient } = require('messaging-api-telegram');
const subDays = require('date-fns/subDays');
const format = require('date-fns/format');
const pMap = require('p-map');

const getDatabase = require('./database');

const botToken = process.env.BOT_TOKEN;
const userIds = process.env.USER_IDS.split(',');

const bot = new TelegramClient({
  accessToken: botToken,
});

const main = async () => {
  const db = await getDatabase();

  const today = new Date();
  const todayDate = new Date(
    Date.UTC(today.getFullYear(), today.getMonth(), today.getDate())
  );

  const excludeBigCompanyCode = [
    '1101',
    '1310',
    '2207',
    '2303',
    '2317',
    '2323',
    '2324',
    '2330',
    '2347',
    '2356',
    '2412',
    '2610',
    '2612',
    '2881',
    '2882',
    '2883',
    '2885',
    '2887',
    '2888',
    '2891',
    '4904',
    '5215',
    '5522',
    '5880',
    '6219',
    '6592',
    '9904',
    '9945',
  ];

  const dailyMessages = await db
    .collection('company_daily_messages')
    .find({
      // date: { $gte: new Date(2021, 0, 1), $lte: new Date(2021, 2, 1) },
      date: { $gte: subDays(todayDate, 2) },
      title: {
        $regex:
          '((?=.*處分)(?!.*存款)(?!.*理財).*|注意交易資訊標準|減資|股利|合併財報|處置)',
      },
      company_code: { $nin: excludeBigCompanyCode },
      typek: { $ne: 'rotc' },
    })
    .sort({ date: -1, time: -1 })
    .toArray();

  const messages = dailyMessages.map((msg) => {
    const title =
      `date: ${format(msg.date, 'yyyy-MM-dd')} ${msg.time}\n` +
      `code: ${msg.company_code}\n` +
      `name: ${msg.company_name}\n` +
      `typek: ${msg.typek}\n` +
      `title: ${msg.title}\n`;

    return {
      title,
      option: {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [
              {
                text: `${msg.company_name}`,
                url: msg.url,
              },
            ],
          ],
        },
      },
    };
  });

  if (messages.length > 0) {
    await pMap(
      messages,
      async (message) => {
        await pMap(userIds, async (userId) => {
          await bot.sendMessage(userId, message.title, message.option);
        });
      },
      { concurrency: 1 }
    );
  } else {
    await pMap(userIds, async (userId) => {
      await bot.sendMessage(
        userId,
        `date: ${format(todayDate, 'yyyy-MM-dd')} has no messages`
      );
    });
  }
};

main()
  .catch(console.error)
  .then(() => process.exit(0));
