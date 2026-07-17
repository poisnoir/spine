const protocol = @import("protocol");
const string = protocol.mad.string;
pub const MadType = protocol.mad.MadType;

// Shared with the wire payload (protocol/src/payloads.zig's
// RegisterEntityPayload) - the same type moving over the wire is what gets
// stored here, so there's one definition instead of spined and every client
// library each keeping their own copy in sync by hand.

pub const ProducerType = enum {
    service,
    publisher,
};

pub const Consumer = struct {
    name: string,
    producer_type: ProducerType,
};

pub const Service = struct {
    name: string = string{},
    inType: MadType = MadType{},
    outType: MadType = MadType{},
};

pub const Publisher = struct {
    name: string = string{},
    outType: MadType = MadType{},
};

pub const Producer = union(enum) {
    service: Service,
    publisher: Publisher,

    pub fn get_type(self: Producer) ProducerType {
        return switch (self) {
            .service => ProducerType.service,
            .publisher => ProducerType.publisher,
        };
    }
};

pub const Entity = union(enum) {
    consumer: Consumer,
    Producer: Producer,
};
