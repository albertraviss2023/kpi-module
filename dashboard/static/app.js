// Configuration
const kpiConfig = {
    ma: {
        endpoint: '/ma',
        kpis: [
            { key: 'pct_new_apps_evaluated_on_time', name: 'New Apps Evaluated On Time', unit: '%' },
            { key: 'pct_renewal_apps_evaluated_on_time', name: 'Renewal Apps Evaluated On Time', unit: '%' },
            { key: 'pct_variation_apps_evaluated_on_time', name: 'Variation Apps Evaluated On Time', unit: '%' },
            { key: 'pct_fir_responses_on_time', name: 'FIR Responses On Time', unit: '%' },
            { key: 'pct_query_responses_evaluated_on_time', name: 'Query Responses Evaluated On Time', unit: '%' },
            { key: 'pct_granted_within_90_days', name: 'Granted Within 90 Days', unit: '%' },
            { key: 'median_duration_continental', name: 'Median Duration Continental', unit: 'Days' }
        ]
    },
    ct: {
        endpoint: '/ct',
        kpis: [
            { key: 'pct_new_apps_evaluated_on_time', name: 'New Apps Evaluated On Time', unit: '%' },
            { key: 'pct_amendment_apps_evaluated_on_time', name: 'Amendment Apps Evaluated On Time', unit: '%' },
            { key: 'pct_gcp_inspections_on_time', name: 'GCP Inspections On Time', unit: '%' },
            { key: 'pct_safety_reports_assessed_on_time', name: 'Safety Reports Assessed On Time', unit: '%' },
            { key: 'pct_gcp_compliant', name: 'GCP Compliant', unit: '%' },
            { key: 'pct_registry_submissions_on_time', name: 'Registry Submissions On Time', unit: '%' },
            { key: 'pct_capa_evaluated_on_time', name: 'CAPA Evaluated On Time', unit: '%' },
            { key: 'avg_turnaround_time', name: 'Average Turnaround Time', unit: 'Days' }
        ]
    },
    gmp: {
        endpoint: '/gmp',
        kpis: [
            { key: 'pct_facilities_inspected_on_time', name: 'Facilities Inspected On Time', unit: '%' },
            { key: 'pct_complaint_inspections_on_time', name: 'Complaint Inspections On Time', unit: '%' },
            { key: 'pct_inspections_waived_on_time', name: 'Inspections Waived On Time', unit: '%' },
            { key: 'pct_facilities_compliant', name: 'Facilities Compliant', unit: '%' },
            { key: 'pct_capa_decisions_on_time', name: 'CAPA Decisions On Time', unit: '%' },
            { key: 'pct_applications_completed_on_time', name: 'Applications Completed On Time', unit: '%' },
            { key: 'avg_turnaround_time', name: 'Average Turnaround Time', unit: 'Days' },
            { key: 'median_turnaround_time', name: 'Median Turnaround Time', unit: 'Days' },
            { key: 'pct_reports_published_on_time', name: 'Reports Published On Time', unit: '%' }
        ]
    }
};

let allData = { ma: [], ct: [], gmp: [] };
let trendChart = null;
let isInitialized = false;

// Main initialization
document.addEventListener('DOMContentLoaded', async () => {
    console.log('DOMContentLoaded: Initializing dashboard');
    
    // Initialize theme toggle
    initThemeToggle();
    
    try {
        await loadAllData();
        setupEventListeners();
        switchTab('ma');
        isInitialized = true;
    } catch (error) {
        console.error('Initialization error:', error);
        showError('Failed to initialize dashboard. Please try again later.');
    }
});

function initThemeToggle() {
    const themeToggle = document.getElementById('theme-toggle');
    const themeIconLight = document.getElementById('theme-icon-light');
    const themeIconDark = document.getElementById('theme-icon-dark');
    
    // Check for saved theme preference or use system preference
    const savedTheme = localStorage.getItem('theme');
    const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    
    if (savedTheme === 'dark' || (!savedTheme && systemPrefersDark)) {
        document.documentElement.classList.add('dark');
        themeIconLight.classList.add('hidden');
        themeIconDark.classList.remove('hidden');
    }
    
    themeToggle.addEventListener('click', () => {
        const isDark = document.documentElement.classList.toggle('dark');
        localStorage.setItem('theme', isDark ? 'dark' : 'light');
        
        if (isDark) {
            themeIconLight.classList.add('hidden');
            themeIconDark.classList.remove('hidden');
        } else {
            themeIconLight.classList.remove('hidden');
            themeIconDark.classList.add('hidden');
        }
        
        // Update chart colors if a chart exists
        if (trendChart) {
            updateChartForTheme();
        }
    });
}

function updateChartForTheme() {
    if (!trendChart) return;
    
    const isDark = document.documentElement.classList.contains('dark');
    
    // Update chart colors based on theme
    trendChart.options.scales.x.grid.color = isDark ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)';
    trendChart.options.scales.y.grid.color = isDark ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)';
    trendChart.options.scales.x.ticks.color = isDark ? '#e5e7eb' : '#4b5563';
    trendChart.options.scales.y.ticks.color = isDark ? '#e5e7eb' : '#4b5563';
    trendChart.options.scales.x.title.color = isDark ? '#f3f4f6' : '#111827';
    trendChart.options.scales.y.title.color = isDark ? '#f3f4f6' : '#111827';
    
    trendChart.update();
}

async function loadAllData() {
    try {
        showLoading();
        const loadingPromises = [
            fetchData('ma'),
            fetchData('ct'),
            fetchData('gmp')
        ];
        await Promise.all(loadingPromises);
        populateYearFilter();
        hideLoading();
    } catch (error) {
        console.error('Error loading data:', error);
        hideLoading();
        showError('Failed to load data. Please check your connection and try again.');
        throw error;
    }
}

async function fetchData(process) {
    try {
        console.log(`Fetching data from ${kpiConfig[process].endpoint}`);
        const response = await fetch(kpiConfig[process].endpoint);
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        // Client-side deduplication as a fallback
        allData[process] = data.reduce((acc, curr) => {
            const existing = acc.find(d => d.quarter === curr.quarter);
            if (!existing || (curr.id && curr.id > existing.id)) {
                return [...acc.filter(d => d.quarter !== curr.quarter), curr];
            }
            return acc;
        }, []);
        
        console.log(`${process} data loaded:`, allData[process]);
        return allData[process];
    } catch (error) {
        console.error(`Error fetching ${process} data:`, error);
        allData[process] = [];
        throw error;
    }
}

function populateYearFilter() {
    const years = new Set();
    Object.values(allData).forEach(data => {
        data.forEach(item => {
            if (item.quarter) {
                years.add(item.quarter.split('Q')[0]);
            }
        });
    });
    
    const yearFilter = document.getElementById('year-filter');
    const sortedYears = [...years].sort((a, b) => b - a);
    yearFilter.innerHTML = '<option value="">All Years</option>' + 
        sortedYears.map(year => `<option value="${year}">${year}</option>`).join('');
}

function switchTab(process) {
    if (!isInitialized) return;
    
    document.querySelectorAll('.tab-content').forEach(tab => tab.classList.add('hidden'));
    document.getElementById(`${process}-tab`).classList.remove('hidden');
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelector(`.tab-btn[data-tab="${process}"]`).classList.add('active');
    
    updateKpiTable(process);
}

function updateKpiTable(process) {
    const year = document.getElementById('year-filter').value;
    const data = year ? allData[process].filter(d => d.quarter?.startsWith(year)) : allData[process];
    const tableBody = document.getElementById(`${process}-kpi-table`);
    
    tableBody.innerHTML = '';
    
    if (data.length === 0) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="4" class="px-6 py-4 text-center text-gray-500 dark:text-gray-400">
                    No data available for selected filter
                </td>
            </tr>
        `;
        return;
    }
    
    const latestQuarter = data.reduce((max, d) => d.quarter > max ? d.quarter : max, '');
    const latestData = data.find(d => d.quarter === latestQuarter) || {};
    
    const prevQuarter = data
        .filter(d => d.quarter < latestQuarter)
        .reduce((max, d) => d.quarter > max ? d.quarter : max, '');
    const prevData = data.find(d => d.quarter === prevQuarter) || {};
    
    kpiConfig[process].kpis.forEach(kpi => {
        const value = latestData[kpi.key] ?? null;
        const prevValue = prevData[kpi.key] ?? null;
        
        let statusClass = '';
        let displayValue = 'N/A';
        let trendIcon = '';
        
        if (value !== null) {
            displayValue = kpi.unit === '%' ? value.toFixed(2) + '%' : value;
            
            if (kpi.unit === '%') {
                if (value < 70) statusClass = 'bg-red-100 text-red-800';
                else if (value < 90) statusClass = 'bg-orange-100 text-orange-800';
                else statusClass = 'bg-green-100 text-green-800';
            }
        }
        
        let trendText = 'N/A';
        if (value !== null && prevValue !== null) {
            if (value > prevValue) {
                trendText = 'Improved';
                trendIcon = '↑';
            } else if (value < prevValue) {
                trendText = 'Declined';
                trendIcon = '↓';
            } else {
                trendText = 'No change';
                trendIcon = '→';
            }
        } else if (value !== null && prevValue === null && prevQuarter) {
            trendText = 'No previous data';
        }
        
        const row = document.createElement('tr');
        row.className = 'hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors';
        row.innerHTML = `
            <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 dark:text-gray-200">
                ${kpi.name}
            </td>
            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                ${kpi.unit}
            </td>
            <td class="px-6 py-4 whitespace-nowrap text-sm ${statusClass} rounded-md text-center" 
                onclick="showTrendChart('${process}', '${kpi.key}', '${kpi.name}', '${kpi.unit}')">
                ${displayValue}
            </td>
            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400 text-center">
                ${trendIcon} ${trendText}
            </td>
        `;
        tableBody.appendChild(row);
    });
}

function showTrendChart(process, kpiKey, kpiName, kpiUnit) {
    console.log('Attempting to show trend chart for:', kpiName);
    
    const modal = document.getElementById('trend-modal');
    const title = document.getElementById('modal-title');
    const canvas = document.getElementById('trend-chart');
    const isDark = document.documentElement.classList.contains('dark');
    
    // First show the modal
    modal.classList.remove('hidden');
    title.textContent = `${kpiName} Trend`;
    
    // Check if Chart.js is available
    if (typeof Chart === 'undefined') {
        console.error('Chart.js is not loaded');
        showError('Charting library not loaded. Please refresh the page.');
        return;
    }
    
    // Check if canvas exists
    if (!canvas) {
        console.error('Canvas element not found');
        showError('Cannot display chart: Canvas element not found');
        return;
    }
    
    // Destroy previous chart if exists
    if (trendChart) {
        trendChart.destroy();
        trendChart = null;
    }
    
    // Get the data for this KPI
    const data = allData[process];
    const labels = data.map(d => d.quarter);
    const values = data.map(d => d[kpiKey] ?? null);
    
    // Determine line color based on latest value
    const latestValue = values[values.length - 1] || 0;
    let lineColor = isDark ? '#93c5fd' : '#2563eb'; // blue
    if (kpiUnit === '%') {
        if (latestValue < 70) lineColor = isDark ? '#fca5a5' : '#dc2626'; // red
        else if (latestValue < 90) lineColor = isDark ? '#fdba74' : '#ea580c'; // orange
        else lineColor = isDark ? '#86efac' : '#16a34a'; // green
    }
    
    // Create the chart
    try {
        const ctx = canvas.getContext('2d');
        
        // Fix disconnected line segments for null values
        const segments = [];
        let currentSegment = [];
        
        values.forEach((value, index) => {
            if (value !== null) {
                currentSegment.push({ x: labels[index], y: value });
            } else if (currentSegment.length > 0) {
                segments.push(currentSegment);
                currentSegment = [];
            }
        });
        
        if (currentSegment.length > 0) {
            segments.push(currentSegment);
        }
        
        trendChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: kpiName,
                    data: values,
                    borderColor: lineColor,
                    borderWidth: 2,
                    tension: 0.1,
                    fill: false,
                    pointBackgroundColor: values.map(v => v === null ? 'transparent' : lineColor),
                    pointBorderColor: values.map(v => v === null ? 'transparent' : lineColor),
                    pointRadius: values.map(v => v === null ? 0 : 5),
                    pointHoverRadius: values.map(v => v === null ? 0 : 7),
                    segment: {
                        borderColor: ctx => segments.some(seg => seg.includes(ctx.p0)) ? lineColor : 'transparent'
                    }
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        labels: {
                            color: isDark ? '#f3f4f6' : '#111827'
                        }
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        callbacks: {
                            label: function(context) {
                                return `${kpiName}: ${context.raw !== null ? context.raw.toFixed(2) : 'N/A'}${kpiUnit}`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        max: kpiUnit === '%' ? 100 : undefined,
                        title: { 
                            display: true, 
                            text: kpiUnit,
                            color: isDark ? '#f3f4f6' : '#111827'
                        },
                        grid: {
                            color: isDark ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)'
                        },
                        ticks: {
                            color: isDark ? '#e5e7eb' : '#4b5563'
                        }
                    },
                    x: {
                        title: { 
                            display: true, 
                            text: 'Quarter',
                            color: isDark ? '#f3f4f6' : '#111827'
                        },
                        grid: {
                            color: isDark ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)'
                        },
                        ticks: {
                            color: isDark ? '#e5e7eb' : '#4b5563',
                            autoSkip: false,
                            maxRotation: 45,
                            minRotation: 45
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Error creating chart:', error);
        showError('Failed to display chart. Please try again.');
    }
}

function closeTrendModal() {
    const modal = document.getElementById('trend-modal');
    modal.classList.add('hidden');
    if (trendChart) {
        trendChart.destroy();
        trendChart = null;
    }
}

function setupEventListeners() {
    // Tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });
    
    // Year filter
    document.getElementById('year-filter').addEventListener('change', () => {
        const activeTab = document.querySelector('.tab-content:not(.hidden)').id.replace('-tab', '');
        updateKpiTable(activeTab);
    });
    
    // Close modal button
    document.getElementById('close-trend-modal').addEventListener('click', closeTrendModal);
}

function showLoading() {
    console.log('Showing loading indicator');
    document.getElementById('loading-overlay').classList.remove('hidden');
}

function hideLoading() {
    console.log('Hiding loading indicator');
    document.getElementById('loading-overlay').classList.add('hidden');
}

function showError(message) {
    console.error('Showing error:', message);
    const errorToast = document.getElementById('error-toast');
    const errorMessage = document.getElementById('error-message');
    
    errorMessage.textContent = message;
    errorToast.classList.remove('hidden');
    
    // Hide after 5 seconds
    setTimeout(() => {
        errorToast.classList.add('hidden');
    }, 5000);
}

// Make functions available globally
window.showTrendChart = showTrendChart;
window.closeTrendModal = closeTrendModal;