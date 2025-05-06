import json
import uuid
import random
import datetime
import boto3
import os

def lambda_handler(event, context):
    """
    Lambda function to generate user funnel events spread over past week and send to Kinesis Data Stream
    """
    # Get parameters from environment variables or use defaults
    stream_name = os.environ.get('STREAM_NAME', 'UserEventStream')
    region = os.environ.get('AWS_REGION', 'us-east-1')
    num_users = int(os.environ.get('NUM_USERS', '10000'))
    
    # Initialize Kinesis client
    kinesis_client = boto3.client('kinesis', region_name=region)
    
    # Define the funnel stages and their respective drop-off rates
    funnel_stages = [
        {"page_name": "landing", "drop_off_rate": 10},  # 100% start
        {"page_name": "login_details", "drop_off_rate": 20},  # 60% continue
        {"page_name": "identity", "drop_off_rate": 6},  # 55% continue
        {"page_name": "finance", "drop_off_rate": 5},  # 50% continue
        {"page_name": "interests", "drop_off_rate": 10},  # 20% continue
        {"page_name": "terms", "drop_off_rate": 15} , # All remaining users complete
        {"page_name": "success", "drop_off_rate": 0} , # All remaining users complete
    ]
    
    # Statistics to track events and user progression
    stats = {
        "total_users_processed": num_users,
        "total_events_sent": 0,
        "page_counts": {},
        "events_by_day": {},
        "campaign_counts": {"campaign1": 0, "campaign2": 0, "none": 0}
    }
    
    # Get current date and calculate date range
    current_date = datetime.datetime.now()
    date_range_start = current_date - datetime.timedelta(days=7)
    
    # Process the specified number of user journeys
    for i in range(num_users):
        user_id = str(uuid.uuid4())
        
        # Randomly select a start date within the past week
        start_date = date_range_start + datetime.timedelta(
            days=random.randint(0, 6),  # 0-6 days after range start
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        # Assign campaign code based on percentages
        # 5% for campaign1, 10% for campaign2, 85% for no campaign
        campaign_random = random.random() * 100  # Random number between 0-100
        if campaign_random < 5:
            campaign_code = "campaign1"
            stats["campaign_counts"]["campaign1"] += 1
        elif campaign_random < 15:  # 5% + 10% = 15%
            campaign_code = "campaign2"
            stats["campaign_counts"]["campaign2"] += 1
        else:
            campaign_code = None
            stats["campaign_counts"]["none"] += 1
        
        user_events = simulate_user_journey(user_id, funnel_stages, start_date, campaign_code)
        
        # Update statistics
        for event in user_events:
            page_name = event["pagename"]
            event_date = event["eventdate"][:10]  # Extract YYYY-MM-DD
            
            # Track page counts
            if page_name in stats["page_counts"]:
                stats["page_counts"][page_name] += 1
            else:
                stats["page_counts"][page_name] = 1
            
            # Track events by day
            if event_date in stats["events_by_day"]:
                stats["events_by_day"][event_date] += 1
            else:
                stats["events_by_day"][event_date] = 1
            
            stats["total_events_sent"] += 1
            
            # Send event to Kinesis
            try:
                kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=json.dumps(event),
                    PartitionKey=event["userid"]
                )
            except Exception as e:
                print(f"Error sending event to Kinesis: {str(e)}")
                # Continue processing other events even if one fails
    
    # Calculate percentages for the funnel
    funnel_stats = []
    for stage in funnel_stages:
        page_name = stage["page_name"]
        count = stats["page_counts"].get(page_name, 0)
        percentage = (count / num_users) * 100
        funnel_stats.append({
            "page_name": page_name,
            "user_count": count,
            "percentage": round(percentage, 1)
        })
    
    # Return execution statistics
    return {
        'statusCode': 200,
        'body': {
            'users_processed': num_users,
            'events_generated': stats["total_events_sent"],
            'funnel_statistics': funnel_stats,
            'events_by_day': stats["events_by_day"],
            'campaign_distribution': stats["campaign_counts"]
        }
    }

def simulate_user_journey(user_id, funnel_stages, start_date, campaign_code):
    """
    Generate events for a single user journey through the funnel
    starting from the provided start date
    """
    events = []
    current_time = start_date
    
    for i, stage in enumerate(funnel_stages):
        # Check if user drops off at this stage
        if i > 0 and random.randint(1, 100) <= funnel_stages[i-1]["drop_off_rate"]:
            break
        
        # Generate event for this stage
        # Users typically take between 1-30 minutes between steps
        event_time = current_time + datetime.timedelta(minutes=random.randint(1, 30))
        current_time = event_time  # Update for next event
        
        event = {
            "userid": user_id,
            "eventname": f"view_{stage['page_name'].lower().replace(' ', '_')}",
            "eventdate": event_time.isoformat(),
            "pagename": stage["page_name"]
        }
        
        # Add campaign code if it exists
        if campaign_code:
            event["campaigncode"] = campaign_code
        
        events.append(event)
    
    return events