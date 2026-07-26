# Weighted costs

Cost is a positive integer bounded by Policy.MaxCost and total Capacity+Burst.
Use cost to represent relative admission pressure, such as one unit for a read
and five for a bulk export. Cost is not money, a billable usage ledger, or a
quota balance.

Choose weights from measured resource use and keep their range small. Large
weights increase fairness discontinuities. Token and window algorithms reject
the entire request when remaining capacity is below cost; they never partially
consume. Concurrency leases reserve the entire cost until release or expiry.
