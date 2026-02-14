/**
 * Newsletter API endpoint using Resend
 *
 * SETUP REQUIRED:
 * 1. Install Resend: npm install resend
 * 2. Add RESEND_API_KEY to your environment variables
 * 3. Enable SSR in astro.config.mjs OR deploy to Vercel/Netlify
 *
 * For Astro SSR, update astro.config.mjs:
 *   export default defineConfig({
 *     output: 'hybrid', // or 'server'
 *     adapter: vercel(), // or netlify()
 *   })
 */

import type { APIRoute } from 'astro';

// TODO: Uncomment when Resend is installed
// import { Resend } from 'resend';
// const resend = new Resend(import.meta.env.RESEND_API_KEY);

export const POST: APIRoute = async ({ request }) => {
  try {
    const { email } = await request.json();

    if (!email || !isValidEmail(email)) {
      return new Response(
        JSON.stringify({ error: 'Invalid email address' }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // TODO: Implement one of these options:

    // OPTION 1: Add to Resend Audience (for newsletters)
    // const audienceId = import.meta.env.RESEND_AUDIENCE_ID;
    // await resend.contacts.create({
    //   email,
    //   audienceId,
    // });

    // OPTION 2: Send welcome email
    // await resend.emails.send({
    //   from: 'Gazzetta Civica <newsletter@tuodominio.it>',
    //   to: email,
    //   subject: 'Benvenuto alla Newsletter di Gazzetta Civica',
    //   html: `
    //     <h1>Grazie per l'iscrizione!</h1>
    //     <p>Riceverai aggiornamenti sulle nuove leggi e analisi legislative.</p>
    //   `,
    // });

    // For now, just log (remove in production)
    console.log('Newsletter signup:', email);

    return new Response(
      JSON.stringify({ success: true }),
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );

  } catch (error) {
    console.error('Newsletter API error:', error);
    return new Response(
      JSON.stringify({ error: 'Internal server error' }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    );
  }
};

function isValidEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}
