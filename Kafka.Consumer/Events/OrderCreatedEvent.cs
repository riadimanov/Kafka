namespace Kafka.Consumer.Events
{
    public record OrderCreatedEvent
    {
        public string OrderCode { get; init; } = default!;
        public int TotalPrice { get; set; }
        public int UserId {  get; init; }
    }
}
