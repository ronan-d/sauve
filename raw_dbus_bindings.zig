pub const sd_bus = @cImport({
    @cInclude("systemd/sd-bus.h");
});

pub const null_error = sd_bus.sd_bus_error{ .name = null, .message = null };
