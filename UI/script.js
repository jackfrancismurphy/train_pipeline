document.addEventListener('DOMContentLoaded', function() {
    // State object to store operator delays
    const state = {
        delayCount: {},
        totalDelays: 0
    };

    // Function to update the leaderboard
    function updateLeaderboard(data) {
        if (!data || !data.leaderboard) {
            console.log('No valid data received');
            return;
        }

        // Get the leaderboard data
        const leaderboard = data.leaderboard;
        
        // Convert leaderboard object directly to array of entries
        const sortedOperators = Object.entries(leaderboard)
            .sort(([,a], [,b]) => b - a);

        // Calculate total delays
        const totalDelays = Object.values(leaderboard).reduce((sum, count) => sum + count, 0);

        // Update DOM
        const leaderboardBody = document.getElementById('leaderboardBody');
        leaderboardBody.innerHTML = '';

        sortedOperators.forEach(([operator, count], index) => {
            const row = document.createElement('tr');
            
            row.innerHTML = `
                <td>${index + 1}</td>
                <td>${operator}</td>
                <td>${count}</td>
            `;

            // Highlight top 3
            if (index < 3) {
                row.classList.add(`rank-${index + 1}`);
            }

            leaderboardBody.appendChild(row);
        });

        // Update timestamp
        document.getElementById('lastUpdate').textContent = 
            new Date().toLocaleTimeString();
    }

    // Function to fetch data from API
    async function fetchLatestData() {
        try {
            const response = await fetch('https://34.169.114.91:8080/api/latest'); // Google external IP address
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
            if (data) {
                updateLeaderboard(data);
            }
        } catch (error) {
            console.error('Error fetching data:', error);
        }
    }

    // Poll for updates every 5 seconds
    function startPolling() {
        // Initial fetch
        fetchLatestData();

        // Set up polling interval
        setInterval(fetchLatestData, 5000);
    }

    // Start the polling when page loads
    startPolling();

    // Expose updateLeaderboard to window for external access
    window.updateLeaderboard = updateLeaderboard; 
});
