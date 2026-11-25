const amqp = require('amqplib');
const user = process.env.AMQP_USER || '';
const pass = process.env.AMQP_PASS || '';
if (!user || !pass) {
  console.error('Error: AMQP_USER and AMQP_PASS environment variables are required');
  process.exit(1);
}
(async () => {
  const c = await amqp.connect({
    hostname: process.env.AMQP_HOST || '192.168.137.44',
    port: Number(process.env.AMQP_PORT || 5672),
    vhost: process.env.AMQP_VHOST || '/',
    username: user,
    password: pass,
  });
  const ch = await c.createChannel();
  await ch.assertExchange('home.automation', 'topic', {durable:true});
  const msg = JSON.stringify({msg_id:'test-0001', ts:new Date().toISOString(), source:'SNS-SW-0001', message_class:'state', payload:{serial_number:'SNS-SW-0001', digital_value:[1]}});
  ch.publish('home.automation', 'home.sensor.1.SNS-SW-0001.state', Buffer.from(msg), {persistent:true});
  console.log('published:', msg);
  await ch.close(); await c.close();
})().catch(e => { console.error(e.message); process.exit(1); });
