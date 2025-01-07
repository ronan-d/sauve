const std = @import("std");

const assert = std.debug.assert;
const panic = std.debug.panic;

const dbus_bindings = @import("raw_dbus_bindings.zig");

const sd_bus = dbus_bindings.sd_bus;

const basic_bus_name = "org.freedesktop.DBus";
const basic_object_path = "/org/freedesktop/DBus";
const basic_interface = basic_bus_name;

const owner_signal_name = "NameOwnerChanged";

fn addSignalHandler(bus: ?*sd_bus.sd_bus) void {
    {
        const res = sd_bus.sd_bus_match_signal(
            bus,
            null,
            basic_bus_name,
            basic_object_path,
            basic_interface,
            owner_signal_name,
            null,
            null,
        );
        assert(0 < res);
    }
}

const notif_bus_name = "org.freedesktop.Notifications";

fn nameOwnerChangedCallback(
    m: ?*sd_bus.sd_bus_message,
) bool {
    const name = blk: {
        var name: [*:0]const u8 = undefined;
        const res = sd_bus.sd_bus_message_read_basic(m, 's', @ptrCast(&name));

        std.debug.print("{}\n", .{res});
        assert(0 < res);
        break :blk name;
    };

    if (std.mem.orderZ(u8, name, notif_bus_name) != std.math.Order.eq) {
        return false;
    }

    const old_owner = blk: {
        var old_owner: [*:0]const u8 = undefined;
        const res = sd_bus.sd_bus_message_read_basic(m, 's', @ptrCast(&old_owner));

        assert(0 < res);

        break :blk old_owner;
    };

    _ = old_owner;

    const new_owner = blk: {
        var new_owner: [*:0]const u8 = undefined;
        const res = sd_bus.sd_bus_message_read_basic(m, 's', @ptrCast(&new_owner));

        assert(0 < res);

        break :blk new_owner;
    };

    if (new_owner[0] == 0) {
        std.debug.print("The notification name now doesn't have an owner\n", .{});
        return false;
    } else {
        std.debug.print("The notification bus name is now owned by {s}\n", .{new_owner});
        return true;
    }
}

pub const Manager = struct {
    bus: ?*sd_bus.sd_bus,

    pub fn init() @This() {
        const bus = blk: {
            var bus: ?*sd_bus.sd_bus = undefined;
            const res = sd_bus.sd_bus_default_user(&bus);

            std.debug.assert(0 <= res);

            break :blk bus.?;
        };

        var self = @This(){ .bus = bus };

        addSignalHandler(self.bus);
        self.askForInitialNameOwner();

        return self;
    }

    pub fn deinit(self: @This()) void {
        const res = sd_bus.sd_bus_flush_close_unref(self.bus);
        assert(res == null);
    }

    fn resetConnection(self: *@This()) void {
        std.debug.print("We have to reset the connection\n", .{});
        self.deinit();
        self.* = @This().init();
    }

    const connreset_code = -104;

    fn askForInitialNameOwner(self: @This()) void {
        const bus = self.bus;

        const request = blk: {
            var request: ?*sd_bus.sd_bus_message = undefined;
            const res = sd_bus.sd_bus_message_new_method_call(
                bus,
                &request,
                basic_bus_name,
                basic_object_path,
                basic_interface,
                "GetNameOwner",
            );

            assert(0 <= res);

            break :blk request;
        };

        defer {
            const res = sd_bus.sd_bus_message_unref(request);
            assert(res == null);
        }

        {
            const res = sd_bus.sd_bus_message_append_basic(request, 's', @ptrCast(notif_bus_name));
            assert(0 <= res);
        }

        {
            const res = sd_bus.sd_bus_call_async(
                bus,
                null,
                request,
                null,
                null,
                std.math.maxInt(u64),
            );
            assert(0 <= res);
        }
    }

    pub fn waitUntilNameOwned(self: *@This()) void {
        while (true) {
            const msg = self.receiveMessage();
            defer {
                const res = sd_bus.sd_bus_message_unref(msg);
                assert(res == null);
            }

            if (processMessage(msg)) {
                return;
            }
        }
    }

    // Receive a message. What's interesting about this function is that it
    // takes care of resetting the connection if it gets interrupted.
    fn receiveMessage(self: *@This()) *sd_bus.sd_bus_message {
        var msg: ?*sd_bus.sd_bus_message = undefined;

        while (true) {
            const res = sd_bus.sd_bus_process(self.bus, &msg);
            if (res == 0) {
                self.waitOnBus();
            } else if (res == connreset_code) {
                self.resetConnection();
            } else if (res < 0) {
                panic("{}\n", .{res});
            } else if (msg) |m| {
                return m;
            }
        }
    }

    // Wait for something to be available on the bus, taking care of resetting
    // the connection if necessary.
    fn waitOnBus(self: *@This()) void {
        const res = sd_bus.sd_bus_wait(self.bus, std.math.maxInt(u64));
        if (res == 0) {
            // That's impossible in principle since we asked to wait indefinitely.
            unreachable;
        } else if (res == connreset_code) {
            self.resetConnection();
        } else if (res < 0) {
            panic("{}", .{res});
        }
    }
};

fn processMessage(msg: *sd_bus.sd_bus_message) bool {
    const t = blk: {
        var t: u8 = undefined;
        const res = sd_bus.sd_bus_message_get_type(msg, &t);
        assert(0 <= res);
        break :blk t;
    };

    return switch (t) {
        sd_bus.SD_BUS_MESSAGE_SIGNAL => blk: {
            const member = sd_bus.sd_bus_message_get_member(msg);
            break :blk if (std.mem.orderZ(u8, member, owner_signal_name) == std.math.Order.eq)
                nameOwnerChangedCallback(msg)
            else
                false;
        },
        sd_bus.SD_BUS_MESSAGE_METHOD_RETURN => processMethodReturn(msg),
        sd_bus.SD_BUS_MESSAGE_METHOD_ERROR => false,
        else => panic("{}\n", .{t}),
    };
}

fn processMethodReturn(msg: *sd_bus.sd_bus_message) bool {
    const owner_name = blk: {
        var owner_name: ?[*:0]const u8 = undefined;
        const res = sd_bus.sd_bus_message_read_basic(msg, 's', @ptrCast(&owner_name));

        assert(0 < res);

        break :blk owner_name;
    };

    if (owner_name) |name| {
        std.debug.print(
            "The notification name is initially owned by {s}\n",
            .{name},
        );
        return true;
    } else {
        std.debug.print(
            "The notification name initially doesn't have an owner\n",
            .{},
        );
        return false;
    }
}
