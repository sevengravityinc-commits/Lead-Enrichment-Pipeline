#!/usr/bin/env node
/**
 * Onboard a new client by sending welcome email
 * Usage: node onboard_client.js <client_email> [--name CLIENT_NAME] [--calendar-link LINK]
 */

import nodemailer from 'nodemailer';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

function loadTemplate(templateName = 'welcome_email.html') {
  const templatePath = join(__dirname, 'templates', templateName);
  return readFileSync(templatePath, 'utf-8');
}

function renderTemplate(templateContent, variables) {
  let rendered = templateContent;
  for (const [key, value] of Object.entries(variables)) {
    rendered = rendered.replaceAll(`{${key}}`, value);
  }
  return rendered;
}

async function sendEmail(to, subject, htmlBody) {
  const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST || 'smtp.gmail.com',
    port: parseInt(process.env.SMTP_PORT || '587'),
    secure: false,
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASSWORD,
    },
  });

  const fromName = process.env.FROM_NAME || 'Seven Gravity';
  const fromEmail = process.env.FROM_EMAIL || process.env.SMTP_USER;

  try {
    const info = await transporter.sendMail({
      from: `"${fromName}" <${fromEmail}>`,
      to: to,
      subject: subject,
      html: htmlBody,
    });

    console.log(`✓ Email sent successfully to ${to}`);
    console.log(`✓ Message ID: ${info.messageId}`);
    return true;
  } catch (error) {
    console.error(`✗ Failed to send email: ${error.message}`);
    return false;
  }
}

async function onboardClient(clientEmail, clientName, calendarLink) {
  console.log(`Onboarding client: ${clientEmail}`);
  console.log(`Client name: ${clientName}`);
  console.log(`Calendar link: ${calendarLink}`);

  try {
    // Load and render template
    const template = loadTemplate();
    const emailBody = renderTemplate(template, {
      client_name: clientName,
      calendar_link: calendarLink,
    });

    // Send email
    const subject = "Welcome to Seven Gravity - Let's Get Started!";
    const success = await sendEmail(clientEmail, subject, emailBody);

    if (success) {
      console.log(`\n✓ Successfully onboarded ${clientEmail}`);
      console.log(`✓ Welcome email sent with calendar link: ${calendarLink}`);
    } else {
      console.log(`\n✗ Failed to onboard ${clientEmail}`);
    }

    return success;
  } catch (error) {
    console.error(`✗ Error: ${error.message}`);
    return false;
  }
}

// Parse command line arguments
const args = process.argv.slice(2);

if (args.length === 0) {
  console.log('Usage: node onboard_client.js <client_email> [--name CLIENT_NAME] [--calendar-link LINK]');
  process.exit(1);
}

const clientEmail = args[0];
let clientName = process.env.DEFAULT_CLIENT_NAME || 'there';
let calendarLink = process.env.CALENDAR_LINK || 'https://calendly.com/sevengravity';

// Parse optional arguments
for (let i = 1; i < args.length; i++) {
  if (args[i] === '--name' && args[i + 1]) {
    clientName = args[i + 1];
    i++;
  } else if (args[i] === '--calendar-link' && args[i + 1]) {
    calendarLink = args[i + 1];
    i++;
  }
}

// Run onboarding
onboardClient(clientEmail, clientName, calendarLink)
  .then(success => process.exit(success ? 0 : 1))
  .catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
