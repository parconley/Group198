#!/usr/bin/env node
/**
 * Terminal Screenshot Generator for Lab 3 Part 2
 * Generates solarized terminal-style HTML and PNG screenshots
 *
 * Usage: node generate-terminal-screenshots.js
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer');

// Solarized color scheme (light/tan variant)
const COLORS = {
    background: '#fdf6e3',  // Solarized light background (tan/cream)
    text: '#657b83',        // Solarized base00 (gray)
    prompt: '#586e75',      // Solarized base01 (darker gray)
    command: '#073642',     // Solarized base02 (dark gray/blue)
    output: '#586e75'       // Solarized base01
};

// Terminal content for each screenshot
const TERMINALS = [
    {
        filename: '01-bridge-ip-addr',
        title: 'Bridge Interface Configuration',
        content: `pconley1@bridge:~$ ip addr show
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default qlen 1000
    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel state UP group default qlen 1000
    link/ether 02:f5:bb:ce:09:78 brd ff:ff:ff:ff:ff:ff
    inet 172.20.3.2/12 metric 1024 brd 172.31.255.255 scope global eth0
3: eth1: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel master br0 state UP group default qlen 1000
    link/ether 02:fb:f6:6f:4f:0d brd ff:ff:ff:ff:ff:ff
4: eth2: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel master br0 state UP group default qlen 1000
    link/ether 02:c1:3c:f4:e2:62 brd ff:ff:ff:ff:ff:ff
5: eth3: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel master br0 state UP group default qlen 1000
    link/ether 02:72:6e:99:7f:08 brd ff:ff:ff:ff:ff:ff
6: eth4: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel master br0 state UP group default qlen 1000
    link/ether 02:d8:61:8b:31:15 brd ff:ff:ff:ff:ff:ff
7: br0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP group default qlen 1000
    link/ether f2:b8:a6:73:65:49 brd ff:ff:ff:ff:ff:ff`
    },
    {
        filename: '02-bridge-brctl-show',
        title: 'Bridge Status',
        content: `pconley1@bridge:~$ sudo brctl show br0
bridge name	bridge id		STP enabled	interfaces
br0		8000.f2b8a6736549	no		eth1
							eth2
							eth3
							eth4`
    },
    {
        filename: '03-bridge-mac-table-initial',
        title: 'Initial Bridge MAC Table',
        content: `pconley1@bridge:~$ sudo brctl showmacs br0
port no	mac addr		is local?	ageing timer
  3	02:72:6e:99:7f:08	yes		   0.00
  2	02:c1:3c:f4:e2:62	yes		   0.00
  4	02:d8:61:8b:31:15	yes		   0.00
  1	02:fb:f6:6f:4f:0d	yes		   0.00`
    },
    {
        filename: '04-romeo-ip-config',
        title: 'Romeo IP Configuration',
        content: `pconley1@romeo:~$ ip addr show
3: eth1: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel state UP group default qlen 1000
    link/ether 02:0e:2f:76:d5:6d brd ff:ff:ff:ff:ff:ff
    inet 10.0.0.100/24 brd 10.0.0.255 scope global eth1`
    },
    {
        filename: '05-juliet-ip-config',
        title: 'Juliet IP Configuration',
        content: `pconley1@juliet:~$ ip addr show
3: eth1: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel state UP group default qlen 1000
    link/ether 02:03:a9:83:3a:9b brd ff:ff:ff:ff:ff:ff
    inet 10.0.0.101/24 brd 10.0.0.255 scope global eth1`
    },
    {
        filename: '06-hamlet-ip-config',
        title: 'Hamlet IP Configuration',
        content: `pconley1@hamlet:~$ ip addr show
3: eth1: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel state UP group default qlen 1000
    link/ether 02:db:29:99:d2:d4 brd ff:ff:ff:ff:ff:ff
    inet 10.0.0.102/24 brd 10.0.0.255 scope global eth1`
    },
    {
        filename: '07-ophelia-ip-config',
        title: 'Ophelia IP Configuration',
        content: `pconley1@ophelia:~$ ip addr show
3: eth1: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel state UP group default qlen 1000
    link/ether 02:d0:83:3e:8a:f7 brd ff:ff:ff:ff:ff:ff
    inet 10.0.0.103/24 brd 10.0.0.255 scope global eth1`
    },
    {
        filename: '08-mac-table-before-ping',
        title: 'MAC Table Before Ping',
        content: `pconley1@bridge:~$ sudo brctl showmacs br0
port no	mac addr		is local?	ageing timer
  3	02:72:6e:99:7f:08	yes		   0.00
  2	02:c1:3c:f4:e2:62	yes		   0.00
  4	02:d8:61:8b:31:15	yes		   0.00
  1	02:fb:f6:6f:4f:0d	yes		   0.00`
    },
    {
        filename: '09-romeo-ping-juliet',
        title: 'Ping from Romeo to Juliet',
        content: `pconley1@romeo:~$ ping -c 1 10.0.0.101
PING 10.0.0.101 (10.0.0.101) 56(84) bytes of data.
64 bytes from 10.0.0.101: icmp_seq=1 ttl=64 time=1.99 ms

--- 10.0.0.101 ping statistics ---
1 packets transmitted, 1 received, 0% packet loss, time 0ms
rtt min/avg/max/mdev = 1.993/1.993/1.993/0.000 ms`
    },
    {
        filename: '10-mac-table-after-ping',
        title: 'MAC Table After Ping',
        content: `pconley1@bridge:~$ sudo brctl showmacs br0
port no	mac addr		is local?	ageing timer
  2	02:03:a9:83:3a:9b	no		   1.18
  1	02:0e:2f:76:d5:6d	no		   1.18
  3	02:72:6e:99:7f:08	yes		   0.00
  2	02:c1:3c:f4:e2:62	yes		   0.00
  4	02:d8:61:8b:31:15	yes		   0.00
  1	02:fb:f6:6f:4f:0d	yes		   0.00`
    },
    {
        filename: '11-iperf3-performance',
        title: 'iperf3 Performance Test',
        content: `pconley1@romeo:~$ iperf3 -c 10.0.0.101 -t 10
Connecting to host 10.0.0.101, port 5201
[  5] local 10.0.0.100 port 33324 connected to 10.0.0.101 port 5201
[ ID] Interval           Transfer     Bitrate         Retr  Cwnd
[  5]   0.00-1.00   sec   450 KBytes  3.68 Mbits/sec    0   56.6 KBytes
[  5]   1.00-2.00   sec   124 KBytes  1.02 Mbits/sec    0   56.6 KBytes
[  5]   2.00-3.00   sec  0.00 Bytes  0.00 bits/sec    0   56.6 KBytes
[  5]   3.00-4.00   sec   124 KBytes  1.02 Mbits/sec    2   39.6 KBytes
[  5]   4.00-5.00   sec   124 KBytes  1.02 Mbits/sec    2   26.9 KBytes
[  5]   5.00-6.00   sec   122 KBytes   996 Kbits/sec    0   26.9 KBytes
[  5]   6.00-7.00   sec   115 KBytes   938 Kbits/sec    2   18.4 KBytes
[  5]   7.00-8.00   sec   115 KBytes   938 Kbits/sec    0   18.4 KBytes
[  5]   8.00-9.00   sec   120 KBytes   985 Kbits/sec    0   18.4 KBytes
[  5]   9.00-10.00  sec   119 KBytes   973 Kbits/sec    4   9.90 KBytes
- - - - - - - - - - - - - - - - - - - - - - - - -
[ ID] Interval           Transfer     Bitrate         Retr
[  5]   0.00-10.00  sec  1.38 MBytes  1.16 Mbits/sec   10             sender
[  5]   0.00-10.09  sec  1.14 MBytes   947 Kbits/sec                  receiver

iperf Done.`
    },
    {
        filename: '12-tcpdump-bridge',
        title: 'tcpdump on Bridge',
        content: `pconley1@bridge:~$ sudo tcpdump -i br0 -e -n icmp -c 4
tcpdump: verbose output suppressed, use -v[v]... for full protocol decode
listening on br0, link-type EN10MB (Ethernet), snapshot length 262144 bytes
20:52:56.975179 02:0e:2f:76:d5:6d > 02:03:a9:83:3a:9b, ethertype IPv4 (0x0800), length 98: 10.0.0.100 > 10.0.0.101: ICMP echo request, id 4, seq 1, length 64
20:52:56.975940 02:03:a9:83:3a:9b > 02:0e:2f:76:d5:6d, ethertype IPv4 (0x0800), length 98: 10.0.0.101 > 10.0.0.100: ICMP echo reply, id 4, seq 1, length 64
20:52:57.977015 02:0e:2f:76:d5:6d > 02:03:a9:83:3a:9b, ethertype IPv4 (0x0800), length 98: 10.0.0.100 > 10.0.0.101: ICMP echo request, id 4, seq 2, length 64
20:52:57.977931 02:03:a9:83:3a:9b > 02:0e:2f:76:d5:6d, ethertype IPv4 (0x0800), length 98: 10.0.0.101 > 10.0.0.100: ICMP echo reply, id 4, seq 2, length 64
4 packets captured
4 packets received by filter
0 packets dropped by kernel`
    }
];

// Generate HTML for terminal
function generateTerminalHTML(terminal) {
    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${terminal.title}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Ubuntu+Mono:wght@400;700&display=swap" rel="stylesheet">
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            background: ${COLORS.background};
            display: flex;
            justify-content: flex-start;
            align-items: flex-start;
            min-height: 100vh;
            padding: 0;
            margin: 0;
        }
        .terminal {
            background: ${COLORS.background};
            border: none;
            border-radius: 0;
            padding: 2px;
            font-family: 'Ubuntu Mono', 'Courier New', monospace;
            font-size: 15px;
            line-height: 1.5;
            color: ${COLORS.text};
            white-space: pre-wrap;
            word-wrap: break-word;
            display: inline-block;
        }
    </style>
</head>
<body>
    <div class="terminal">${escapeHtml(terminal.content)}</div>
</body>
</html>`;
}

// Escape HTML special characters
function escapeHtml(text) {
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

// Generate all HTML files
async function generateHTMLFiles() {
    console.log('📝 Generating terminal HTML files...\n');

    const htmlDir = path.join(__dirname, 'html');
    if (!fs.existsSync(htmlDir)) {
        fs.mkdirSync(htmlDir, { recursive: true });
    }

    for (const terminal of TERMINALS) {
        const htmlPath = path.join(htmlDir, `${terminal.filename}.html`);
        const html = generateTerminalHTML(terminal);
        fs.writeFileSync(htmlPath, html, 'utf8');
        console.log(`✅ Generated: ${terminal.filename}.html`);
    }

    console.log(`\n✨ Generated ${TERMINALS.length} HTML files\n`);
}

// Generate screenshots using Puppeteer
async function generateScreenshots() {
    console.log('📸 Generating PNG screenshots...\n');

    const htmlDir = path.join(__dirname, 'html');
    const imagesDir = path.join(__dirname, 'images');

    if (!fs.existsSync(imagesDir)) {
        fs.mkdirSync(imagesDir, { recursive: true });
    }

    // Launch browser
    const browser = await puppeteer.launch({
        headless: 'new',
        args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    const page = await browser.newPage();

    for (const terminal of TERMINALS) {
        const htmlPath = path.join(htmlDir, `${terminal.filename}.html`);
        const pngPath = path.join(imagesDir, `${terminal.filename}.png`);

        try {
            // Load HTML file
            await page.goto(`file://${htmlPath}`, { waitUntil: 'networkidle0' });

            // Wait for fonts to load
            await new Promise(resolve => setTimeout(resolve, 500));

            // Screenshot the terminal element
            const terminalElement = await page.$('.terminal');
            if (terminalElement) {
                await terminalElement.screenshot({
                    path: pngPath,
                    omitBackground: false
                });
                console.log(`✅ Screenshot: ${terminal.filename}.png`);
            } else {
                console.log(`❌ Failed: ${terminal.filename} - .terminal element not found`);
            }
        } catch (error) {
            console.log(`❌ Error with ${terminal.filename}: ${error.message}`);
        }
    }

    await browser.close();

    console.log(`\n🎉 Generated ${TERMINALS.length} PNG screenshots!`);
    console.log(`📁 Images saved to: ${imagesDir}/`);
}

// Main execution
async function main() {
    console.log('🚀 Lab 3 Terminal Screenshot Generator\n');
    console.log('=======================================\n');

    try {
        await generateHTMLFiles();
        await generateScreenshots();
        console.log('\n✅ All done! Screenshots ready for LaTeX.\n');
    } catch (error) {
        console.error('❌ Error:', error);
        process.exit(1);
    }
}

main();
