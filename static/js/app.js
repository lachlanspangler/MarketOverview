document.addEventListener("DOMContentLoaded", async () => {
    const root = document.getElementById("root");
    
    // Create a container for the table
    const tableContainer = document.createElement("div");
    tableContainer.classList.add("table-container");
    root.appendChild(tableContainer);

    try {
        // Fetch data from the Flask API endpoint
        const response = await fetch("/api/breadth_data");
        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
        }

        const data = await response.json();

        // Check if data is available
        if (data.length === 0) {
            tableContainer.innerHTML = "<p>No data available.</p>";
            return;
        }

        // Create a table element
        const table = document.createElement("table");
        table.classList.add("data-table");

        // Create the table header
        const headerRow = document.createElement("tr");
        ["Index Name", "Multiplier", "Timespan", "Declining", "Unchanged", "Advancing", "Timestamp"].forEach(headerText => {
            const th = document.createElement("th");
            th.textContent = headerText;
            headerRow.appendChild(th);
        });
        table.appendChild(headerRow);

        // Populate the table rows with data
        data.forEach(row => {
            const tr = document.createElement("tr");
            ["index_name", "multiplier", "timespan", "declining", "unchanged", "advancing", "timestamp"].forEach(key => {
                const td = document.createElement("td");
                td.textContent = row[key];
                tr.appendChild(td);
            });
            table.appendChild(tr);
        });

        tableContainer.appendChild(table);
    } catch (error) {
        console.error("Error fetching data:", error);
        tableContainer.innerHTML = `<p>Error loading data: ${error.message}</p>`;
    }
});
