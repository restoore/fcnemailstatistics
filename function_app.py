import azure.functions as func
import logging
import msal
import aiohttp
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict

# Initialize the Azure Function App
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="http_trigger")
async def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Retrieve the 'email' parameter from the query string or request body
    email = req.params.get('email')
    if not email:
        try:
            req_body = req.get_json()
        except ValueError:
            req_body = None
        if req_body:
            email = req_body.get('email')

    if email:
        # Application parameters (replace with your own information)
        CLIENT_ID = 'your-client-id'
        CLIENT_SECRET = 'your-client-secret'
        TENANT_ID = 'your-tenant-id'

        # User information (email of the target user)
        USER_EMAIL = email
        logging.info(f"Processing email for user: {USER_EMAIL}")

        # Date range (format YYYY-MM-DD)
        START_DATE = req.params.get('start_date')
        END_DATE = req.params.get('end_date')

        # Format dates in ISO8601
        if START_DATE:
            start_date_iso = datetime.strptime(START_DATE, '%Y-%m-%d').isoformat() + 'Z'
        else:
            start_date_iso = (datetime.now() - timedelta(days=5)).isoformat() + 'Z'  # Default to 5 days ago
            logging.info(f"No start date provided. Using default start date: {start_date_iso}")

        if END_DATE:
            end_date_iso = datetime.strptime(END_DATE, '%Y-%m-%d').isoformat() + 'Z'
        else:
            end_date_iso = datetime.now().isoformat() + 'Z'  # Default to now
            logging.info(f"No end date provided. Using default end date: {end_date_iso}")

        # Authority and scope for authentication
        AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
        SCOPE = ["https://graph.microsoft.com/.default"]

        # Obtain the access token
        logging.info('Obtaining access token from Azure AD.')
        msal_app = msal.ConfidentialClientApplication(
            CLIENT_ID,
            authority=AUTHORITY,
            client_credential=CLIENT_SECRET
        )
        result = msal_app.acquire_token_for_client(scopes=SCOPE)

        if 'access_token' in result:
            access_token = result['access_token']
            logging.info('Access token acquired successfully.')
        else:
            error_message = f"Error obtaining access token: {result.get('error_description')}"
            logging.error(error_message)
            return func.HttpResponse(
                error_message,
                status_code=500
            )

        headers = {
            'Authorization': f"Bearer {access_token}",
            'Content-Type': 'application/json'
        }

        # Build the request URL to filter emails by date and expand properties
        logging.info('Building request URL to retrieve messages.')
        url = (
            f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/messages"
            f"?$expand=singleValueExtendedProperties($filter=Id eq 'Long 0x0E08')"
            f"&$filter=receivedDateTime ge {start_date_iso} and receivedDateTime le {end_date_iso}"
            f"&$top=100"
        )

        emails_count = 0
        messages_size_total = 0
        attachment_counts = defaultdict(int)  # Counts per contentType
        attachment_sizes = defaultdict(int)   # Sizes per contentType

        # Use an asynchronous session
        async with aiohttp.ClientSession() as session:
            # Pass the session and headers to the processing function
            emails_data = await get_all_messages(session, headers, url)

            # Process all emails concurrently
            tasks = [process_email(session, headers, message, USER_EMAIL) for message in emails_data]
            results = await asyncio.gather(*tasks)

            # Aggregate the results
            for result in results:
                emails_count += 1
                messages_size_total += result['message_size']
                # Update attachment counts and sizes per contentType
                for content_type, count in result['attachment_counts'].items():
                    attachment_counts[content_type] += count
                for content_type, size in result['attachment_sizes'].items():
                    attachment_sizes[content_type] += size

        # Convert sizes from bytes to megabytes (MB)
        attachments_size_total_mb = messages_size_total / (1024 * 1024)
        messages_size_total_mb = messages_size_total / (1024 * 1024)

        logging.info(f"Total number of emails: {emails_count}")
        logging.info(f"Total size of messages: {messages_size_total_mb:.2f} MB")
        logging.info("Attachment counts and sizes by contentType:")
        for content_type in attachment_counts:
            size_mb = attachment_sizes[content_type] / (1024 * 1024)
            logging.info(f"{content_type}: Count = {attachment_counts[content_type]}, Size = {size_mb:.2f} MB")

        # Build the HTML content to return
        attachment_table_rows = ''
        for content_type in attachment_counts:
            count = attachment_counts[content_type]
            size_mb = attachment_sizes[content_type] / (1024 * 1024)
            attachment_table_rows += f"""
            <tr>
                <td>{content_type}</td>
                <td>{count}</td>
                <td>{size_mb:.2f} MB</td>
            </tr>
            """

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Email Processing Results</title>
            <style>
                table {{
                    border-collapse: collapse;
                    width: 50%;
                }}
                th, td {{
                    text-align: left;
                    padding: 8px;
                }}
                th {{
                    background-color: #4caf50;
                    color: white;
                }}
                tr:nth-child(even) {{background-color: #f2f2f2;}}
            </style>
        </head>
        <body>
            <h1>Email Processing Results for {USER_EMAIL}</h1>
            <p>Total number of emails: {emails_count}</p>
            <p>Total size of messages: {messages_size_total_mb:.2f} MB</p>
            <h2>Attachments by Content Type</h2>
            <table border="1">
                <tr>
                    <th>Content Type</th>
                    <th>Count</th>
                    <th>Total Size</th>
                </tr>
                {attachment_table_rows}
            </table>
        </body>
        </html>
        """

        # Return the HTML response
        return func.HttpResponse(html_content, mimetype="text/html")

    else:
        # No email address was provided in the request
        logging.warning('No email address provided in the request.')
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Please provide an 'email' "
            "parameter in the query string or in the request body.",
            status_code=200
        )

async def get_all_messages(session, headers, url):
    messages = []
    while url:
        logging.info(f'Retrieving messages from URL: {url}')
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                error_message = (
                    f"Error retrieving emails: status code {response.status}, "
                    f"message {await response.text()}"
                )
                logging.error(error_message)
                break

            data = await response.json()
            messages.extend(data.get('value', []))
            logging.info(f"Retrieved {len(data.get('value', []))} messages.")

            # Handle pagination if necessary
            url = data.get('@odata.nextLink', None)
            if url:
                logging.info('More messages to retrieve, continuing to next page.')
            else:
                logging.info('No more messages to retrieve.')
                break
    return messages

async def process_email(session, headers, message, USER_EMAIL):
    # Initialize counts for this email
    message_size = 0
    attachment_counts = defaultdict(int)  # Counts per contentType
    attachment_sizes = defaultdict(int)   # Sizes per contentType
    email_attachment_counts = defaultdict(int)
    email_attachment_sizes = defaultdict(int)

    # Extract the message size from the extended property
    extended_properties = message.get('singleValueExtendedProperties', [])
    for prop in extended_properties:
        if prop.get('id') == 'Long 0xe08':
            message_size = int(prop.get('value', 0))
            break
    
    if(message.get('hasAttachments', False) == True) :
        # Check for PDF attachments in the message
        attachments_url = (
            f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/messages/"
            f"{message['id']}/attachments?$top=50"
        )
        email_attachment_counts, email_attachment_sizes  = await get_pdf_attachments(session, headers, attachments_url)

    # Update the counts and sizes
    for content_type, count in email_attachment_counts.items():
        attachment_counts[content_type] += count
    for content_type, size in email_attachment_sizes.items():
        attachment_sizes[content_type] += size

    return {
        'message_size': message_size,
        'attachment_counts': attachment_counts,
        'attachment_sizes': attachment_sizes
    }

async def get_pdf_attachments(session, headers, url):
    attachment_counts = defaultdict(int)  # Counts per contentType
    attachment_sizes = defaultdict(int)   # Sizes per contentType
    max_retries = 5  # Maximum number of retries for 429 status
    retry_count = 0

    while url:
        async with session.get(url, headers=headers) as response:
            if response.status == 429:
                if retry_count < max_retries:
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        delay = int(retry_after)
                    else:
                        delay = 10  # Default delay of 10 seconds
                    logging.warning(f"Received 429 Too Many Requests. Retrying after {delay} seconds...")
                    await asyncio.sleep(delay)
                    retry_count += 1
                    continue
                else:
                    logging.error("Maximum retries reached for 429 Too Many Requests.")
                    break
            elif response.status != 200:
                logging.error(f"Error retrieving attachments: {response.status}")
                break
            else:
                data = await response.json()
                attachments = data.get('value', [])

                for attachment in attachments:
                    if attachment.get('@odata.type') == '#microsoft.graph.fileAttachment':
                        content_type = attachment.get('contentType', 'unknown')
                        size = int(attachment.get('size', 0))
                        attachment_counts[content_type] += 1
                        attachment_sizes[content_type] += size

                # Handle pagination if necessary
                url = data.get('@odata.nextLink', None)
                retry_count = 0  # Reset retry count after a successful request
                if not url:
                    break

    return attachment_counts, attachment_sizes
