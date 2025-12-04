# Power BI Dashboard - Configuration Guide

This directory contains Power BI reports that connect to the Census Bureau Common Crawl Analysis Databricks workspace. After deploying the project to your own Databricks instance, you'll need to update the Power Query connections to point to your workspace.

## 📋 Prerequisites

Before configuring Power BI, ensure you have:

1. **Deployed Databricks Workspace**: Complete deployment of `cc_segement_ingestion` and `nlp_pipeline` to your Databricks instance
2. **Unity Catalog Access**: Access to the catalog containing your data (default: `census_bureau_capstone`)
3. **Databricks Personal Access Token (PAT)**: Required for authentication
4. **Power BI Desktop**: Latest version installed on your machine

## 🔑 Step 1: Generate Databricks Personal Access Token

1. Log in to your Databricks workspace
2. Click on your username in the top-right corner
3. Select **Settings**
4. Select **Developer** under the User grouping
4. Navigate to **Access tokens** tab
5. Click **Generate new token**
6. Provide a comment (e.g., "Power BI Connection") and set expiration
7. Click **Generate**
8. **Copy and save the token immediately** - you won't be able to see it again

📚 [Databricks Personal Access Tokens Documentation](https://docs.databricks.com/en/dev-tools/auth/pat.html)

## 🔌 Step 2: Locate Your Databricks Connection Details

You'll need the following information from your Databricks workspace:

### Server Hostname
1. In Databricks, go to **Compute** → Select your cluster
2. Click **Connection Details** tab
3. Copy the **Server Hostname** (format: `your-workspace.cloud.databricks.com`)

### HTTP Path
1. In the same **Connection Details** tab
2. Copy the **HTTP Path** (format: `/sql/1.0/warehouses/xxxxx` for SQL warehouses or `/sql/protocolv1/o/xxxxx/xxxxx` for clusters)

📚 [Databricks Connection Details Documentation](https://docs.databricks.com/en/integrations/jdbc-odbc-bi.html)

## 🔄 Step 3: Update Power Query Connections

1. **Open the Power BI file** (`.pbix`)
2. Click **Home** → **Transform data** to open Power Query Editor
3. In Power Query Editor, click **Home** → **Data source settings**
4. Select the existing Databricks connection
5. Click **Change Source...**
6. Update the following fields:
   - **Server**: Your Databricks server hostname
   - **HTTP Path**: Your SQL warehouse or cluster HTTP path
7. Click **OK**
8. Click **Data source settings**
9. Click **Edit Permissions...**
10. Under **Credentials**, click **Edit...**
11. Select **Database** authentication
    - **User name**: `token`
    - **Password**: Your Databricks Personal Access Token
12. Click **OK** to save
13. Click **Close** and then **Close & Apply** in Power Query Editor
14. **Refresh** your data to test the connection

## 📊 Step 4: Verify Table Connections

After updating connections, verify that all tables are accessible:

### Expected Tables in Gold Schema

- `census_repackaged_enriched`: Classified content (cites vs. repackages)
- `unigrams_repackaged`: N-gram analysis results
- `nlp_pipeline_summary`: Pipeline execution summaries

### Expected Tables in Silver Schema

- `cleaned_master_crawls_2025`: Filtered 2025 master indexes
- `census_product_cleaned`: Filtered and processed Census-related web content

If any tables are missing:
1. Verify the table exists in your Unity Catalog
2. Check that the pipeline jobs have run successfully
3. Confirm you have read permissions on the catalog/schema

## 🔒 Security Best Practices

1. **Token Management**:
   - Set appropriate expiration dates for PATs (recommended: 90 days)
   - Store tokens securely (use password manager)
   - Rotate tokens regularly
   - Revoke tokens when no longer needed

2. **Access Control**:
   - Grant minimum required permissions (READ on catalog/schema)
   - Use service accounts for shared reports
   - Implement row-level security if needed

3. **Publishing to Power BI Service**:
   - When publishing to Power BI Service, you'll need to configure a gateway or use DirectQuery
   - Store credentials securely using Power BI Service data source settings

📚 [Power BI Security Best Practices](https://learn.microsoft.com/en-us/power-bi/enterprise/service-admin-power-bi-security)

## 🚨 Troubleshooting

### "Unable to connect" Error
- Verify server hostname and HTTP path are correct
- Ensure your Databricks cluster/warehouse is running
- Check that your PAT is valid and not expired
- Verify network connectivity to Databricks workspace

### "Table not found" Error
- Confirm the catalog and schema names are correct
- Verify the tables exist by querying them in Databricks SQL
- Check Unity Catalog permissions (READ access required)

### Authentication Errors
- Ensure username is `token` (literal string)
- Password should be your PAT, not your Databricks password
- Check that PAT hasn't been revoked

### Performance Issues
- Use SQL warehouse instead of all-purpose cluster for better performance
- Consider using DirectQuery instead of Import mode for large datasets
- Implement incremental refresh for large tables
- Add filters in Power Query to reduce data volume

## 🔗 Additional Resources

### Databricks Documentation
- [Connect to Databricks from Power BI](https://docs.databricks.com/en/partners/bi/power-bi.html)
- [Unity Catalog and Power BI](https://docs.databricks.com/en/integrations/unity-catalog/power-bi.html)
- [Databricks Partner Connect](https://docs.databricks.com/en/integrations/partner-connect.html)

### Microsoft Documentation
- [Power BI Databricks Connector](https://learn.microsoft.com/en-us/power-query/connectors/databricks)
- [Power BI Data Source Management](https://learn.microsoft.com/en-us/power-bi/connect-data/desktop-data-sources)
- [DirectQuery with Power BI](https://learn.microsoft.com/en-us/power-bi/connect-data/desktop-directquery-about)

## 📝 Support

For issues specific to:
- **Databricks connection**: Check Databricks workspace logs and Unity Catalog permissions
- **Data pipeline issues**: Review job logs in Databricks Workflows
- **Power BI questions**: Consult Power BI documentation or your organization's BI team

---

**Last Updated**: 12/4/2024
**Maintainer**: mrj60@georgetown.edu
