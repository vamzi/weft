//! Identity resolution: expand a principal's **direct** group memberships into the **transitive**
//! [`Identity`] the [`Evaluator`](crate::Evaluator) consumes.
//!
//! Groups can nest (a group is a member of another group), and a grant to an outer group applies
//! to members of inner groups. The metastore stores direct edges (`group_members`); this resolves
//! the closure once, at session start, so the evaluator can treat the group set as ground truth.

use std::collections::{HashMap, HashSet};

use crate::Identity;

/// The direct membership graph: for each group, the groups and users that are *direct* members.
/// Built from the `group_members` table.
#[derive(Debug, Default, Clone)]
pub struct MembershipGraph {
    /// group → directly-contained child groups.
    child_groups: HashMap<String, Vec<String>>,
    /// group → directly-contained users.
    user_members: HashMap<String, Vec<String>>,
}

impl MembershipGraph {
    /// An empty graph.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `child_group` is a direct member of `group`.
    pub fn add_subgroup(&mut self, group: impl Into<String>, child_group: impl Into<String>) {
        self.child_groups
            .entry(group.into())
            .or_default()
            .push(child_group.into());
    }

    /// Record that `user` is a direct member of `group`.
    pub fn add_user(&mut self, group: impl Into<String>, user: impl Into<String>) {
        self.user_members
            .entry(group.into())
            .or_default()
            .push(user.into());
    }

    /// The transitive set of groups `user` belongs to: every group that contains the user
    /// directly, plus every group that (transitively) contains one of those groups.
    pub fn groups_of_user(&self, user: &str) -> Vec<String> {
        // Seed with groups directly containing the user.
        let mut frontier: Vec<String> = self
            .user_members
            .iter()
            .filter(|(_, users)| users.iter().any(|u| u == user))
            .map(|(g, _)| g.clone())
            .collect();

        let mut seen: HashSet<String> = frontier.iter().cloned().collect();
        // Walk *up*: a group G is also reached if G directly contains a group already reached.
        // Iterate to a fixpoint over the parent edges.
        loop {
            let mut added = Vec::new();
            for (parent, children) in &self.child_groups {
                if seen.contains(parent) {
                    continue;
                }
                if children.iter().any(|c| seen.contains(c)) {
                    added.push(parent.clone());
                }
            }
            if added.is_empty() {
                break;
            }
            for g in added {
                if seen.insert(g.clone()) {
                    frontier.push(g);
                }
            }
        }
        frontier.sort();
        frontier.dedup();
        frontier
    }

    /// Build the [`Identity`] the evaluator consumes for `user`, with groups transitively resolved.
    pub fn resolve(&self, user: &str) -> Identity {
        Identity::user(user).with_groups(self.groups_of_user(user))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `bob` ∈ analysts; analysts ⊂ data-team; data-team ⊂ everyone. bob inherits all three.
    #[test]
    fn nested_groups_resolve_transitively() {
        let mut g = MembershipGraph::new();
        g.add_user("analysts", "bob");
        g.add_subgroup("data-team", "analysts");
        g.add_subgroup("everyone", "data-team");

        let id = g.resolve("bob");
        assert_eq!(id.user.as_deref(), Some("bob"));
        let mut groups = id.groups.clone();
        groups.sort();
        assert_eq!(groups, vec!["analysts", "data-team", "everyone"]);
    }

    #[test]
    fn unrelated_user_gets_no_groups() {
        let mut g = MembershipGraph::new();
        g.add_user("analysts", "bob");
        assert!(g.resolve("eve").groups.is_empty());
    }

    /// A membership cycle must not loop forever; the fixpoint walk terminates.
    #[test]
    fn cycles_terminate() {
        let mut g = MembershipGraph::new();
        g.add_user("a", "bob");
        g.add_subgroup("a", "b");
        g.add_subgroup("b", "a"); // cycle
        let groups = g.resolve("bob").groups;
        assert!(groups.contains(&"a".to_string()));
        assert!(groups.contains(&"b".to_string()));
    }
}
