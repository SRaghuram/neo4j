MERGE (planTree:PlanTree {description_hash:{plan_description_hash}})
ON CREATE SET
    planTree.description={plan_description}
RETURN planTree
