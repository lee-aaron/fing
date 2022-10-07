// Enums and Flags that will be sent over the wire to handle parsing the packets
pub enum FileConfig {
    // Create
    Create {},
    // Read
    Read {},
    // Update
    Update { version: u16 },
    // Delete
    Delete {},
    // List
    List {},
}

