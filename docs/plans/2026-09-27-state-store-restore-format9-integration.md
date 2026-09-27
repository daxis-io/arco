# Private restore integration with authority format 9

This integration ports the existing Step 3 private restore components onto main after the authority format 9 hard cut. It does not migrate authority format 8 data or enable production bounded restore advancement.

Production Arrow segments now have a ninth, nullable `expires_at_ms: Int64` column. Restore preflight accepts that exact format 9 segment shape and rejects the former eight-column shape. Its buffer grammar and allocation envelopes account for the ninth column. The private restore entry points remain gated to synthetic authority format 8, and the shared restore payload reader rejects any expiry hint before producing an output. Preserving expiry through merge, singleton comparison, logical history, and final verification is required before format 9 authority can use those entry points.

Terminal candidate publication remains unsupported. A passing private component or unit test does not establish a prepared candidate, HEAD publication, public workspace restore, or provider qualification.
