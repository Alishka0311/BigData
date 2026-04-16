package ru.bdsnowflake.lr3;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public record PetRecord(
        String petKey,
        String customerKey,
        String petType,
        String petName,
        String petBreed,
        String petCategory
) {
    public static PetRecord fromEvent(RawSaleEvent event) {
        CustomerRecord customer = CustomerRecord.fromEvent(event);
        return new PetRecord(
                PetShopStreamingJob.stableKey(
                        "pet",
                        customer.customerKey(),
                        event.customer_pet_type,
                        event.customer_pet_name,
                        event.customer_pet_breed
                ),
                customer.customerKey(),
                event.customer_pet_type,
                event.customer_pet_name,
                event.customer_pet_breed,
                event.pet_category
        );
    }

    public static void bind(PreparedStatement statement, PetRecord record) throws SQLException {
        statement.setString(1, record.petKey);
        statement.setString(2, record.customerKey);
        statement.setString(3, record.petType);
        statement.setString(4, record.petName);
        statement.setString(5, record.petBreed);
        statement.setString(6, record.petCategory);
    }
}
