// site.js: Vanilla JS renderer for Context Reviewer reports

// Determine path to data based on current location depth
const isTopicPage = window.location.pathname.includes('/topics/');
const DATA_URL = isTopicPage ? '../data/report.json' : 'data/report.json';

// Helper: Format Percentage
function fmtPct(val) {
    if (val === null || val === undefined) return 'N/A';
    return (val * 100).toFixed(1) + '%';
}

// Helper: Format Float
function fmtFloat(val) {
    if (val === null || val === undefined) return 'N/A';
    return val.toFixed(3);
}

// Helper: Get Sentiment Class
function getSentClass(val) {
    if (val < -0.2) return 'neg';
    if (val > 0.2) return 'pos';
    return 'neu';
}

// Core Loader
async function loadData() {
    try {
        const resp = await fetch(DATA_URL);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        return await resp.json();
    } catch (e) {
        console.error("Failed to load report data:", e);
        document.body.innerHTML += `<div style="color:red; padding:20px; font-weight:bold;">Error loading data: ${e.message}</div>`;
        return null;
    }
}

// Render: Executive Summary (report.html)
async function renderReport() {
    const data = await loadData();
    if (!data) return;

    // 1. Metadata Headers
    const metaBox = document.getElementById('meta-info');
    if (metaBox) {
        metaBox.innerHTML = `
            Generated: ${new Date(data.generated_at).toLocaleString()}<br>
            Items Analyzed: ${data.dataset.total_items}
        `;
    }

    // 2. Key Findings (Dynamic Text)
    const findingsList = document.getElementById('key-findings-list');
    if (findingsList && data.topics.length > 0) {
        const topTopic = data.topics[0];
        const intrusionRate = fmtPct(data.intrusion.sensitive_intrusion_rate);

        findingsList.innerHTML = `
            <li><strong>Primary Focus:</strong> The most frequent topic is <strong>${topTopic.topic}</strong> (${topTopic.count} items).</li>
            <li><strong>Hostility Index:</strong> ${fmtPct(topTopic.hostility_rate)} of comments in the primary topic score below -0.6.</li>
            <li><strong>Topic Intrusion:</strong> Detected a <strong>${intrusionRate}</strong> rate of introducing sensitive topics into unrelated threads.</li>
        `;
    }

    // 3. Topic Table
    const topicBody = document.querySelector('#topic-table tbody');
    if (topicBody) {
        topicBody.innerHTML = '';
        data.topics.forEach(t => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td><a href="topics/${getTopicLink(t.topic)}">${t.topic}</a></td>
                <td>${t.count}</td>
                <td>${fmtPct(t.percent_of_total)}</td>
                <td class="${getSentClass(t.avg_sentiment)}">${fmtFloat(t.avg_sentiment)}</td>
                <td>${fmtPct(t.hostility_rate)}</td>
            `;
            topicBody.appendChild(row);
        });
    }

    // 4. Intrusion Examples
    const intrusionBody = document.querySelector('#intrusion-table tbody');
    if (intrusionBody && data.intrusion.examples) {
        intrusionBody.innerHTML = '';
        // Limit to 10
        data.intrusion.examples.slice(0, 10).forEach(ex => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${ex.injected_topic}</td>
                <td>${ex.parent_topic || 'None'}</td>
                <td><small>${ex.id}</small></td>
            `;
            intrusionBody.appendChild(row);
        });

        // Update stats text
        const intrusionStats = document.getElementById('intrusion-stats');
        if (intrusionStats) {
            intrusionStats.innerText = `Intrusion Rate: ${fmtPct(data.intrusion.sensitive_intrusion_rate)} (${data.intrusion.examples.length} events)`;
        }
    }
}

// Render: Topic Page (topics/*.html)
async function renderTopicPage(targetTopicKey) {
    const data = await loadData();
    if (!data) return;

    // Find the specific topic stats
    // Note: The key in JSON is full "geopolitics_israel", we might map from filename "israel" or pass full key.
    // I'll assume the page calls this with the full taxonomical key.

    const tData = data.topics.find(t => t.topic === targetTopicKey);
    const container = document.getElementById('topic-deep-dive');

    if (!tData) {
        if (container) container.innerHTML = `<p>No data found for topic: ${targetTopicKey}</p>`;
        return;
    }

    if (container) {
        container.innerHTML = `
            <div class="box">
                <h3>Topic Metrics</h3>
                <ul>
                    <li><strong>Total Mentions:</strong> ${tData.count}</li>
                    <li><strong>Share of Corpus:</strong> ${fmtPct(tData.percent_of_total)}</li>
                    <li><strong>Average Sentiment:</strong> <span class="${getSentClass(tData.avg_sentiment)}">${fmtFloat(tData.avg_sentiment)}</span></li>
                    <li><strong>Hostility Rate:</strong> ${fmtPct(tData.hostility_rate)}</li>
                </ul>
            </div>
            
            <h3>Example IDs</h3>
            <div style="max-height: 200px; overflow-y: auto; background: #eee; padding: 10px; font-family: monospace;">
                ${tData.example_ids.join(', ')}
            </div>
        `;
    }
}

// Render: Contradictions
async function renderContradictions() {
    const data = await loadData();
    if (!data) return;

    const list = document.getElementById('claims-list');
    if (list) {
        if (data.self_portrayal.claims.length === 0) {
            list.innerHTML = '<li>No explicit self-portrayal claims detected in current sample.</li>';
        } else {
            list.innerHTML = '';
            data.self_portrayal.claims.forEach(c => {
                const li = document.createElement('li');
                li.innerHTML = `
                    <strong>[${c.id}]</strong> "${escapeHtml(c.text_excerpt)}..." 
                    <br><small style="color:#666">${c.timestamp || 'Unknown Date'}</small>
                `;
                list.appendChild(li);
            });
        }
    }
}

// Utilities
function getTopicLink(topic) {
    if (topic.includes('israel')) return 'israel.html';
    if (topic.includes('race')) return 'race.html';
    if (topic.includes('religion')) return 'religion.html';
    return '#';
}

function escapeHtml(text) {
    if (!text) return '';
    return text
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}
